package com.haima.sage.bigdata.etl

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.util.Timeout
import com.haima.sage.bigdata.etl.codec.LineCodec
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.typesafe.config.ConfigFactory
import org.junit.{After, Test}

import scala.concurrent.Await

@Test
class Sender {

  import akka.pattern.ask

  import scala.concurrent.duration._

  implicit val timeout = Timeout(1 minutes)
  val system =
    ActorSystem("local", ConfigFactory.load("local.conf"))
  val remote: ActorRef = system.actorOf(Props.create(classOf[Exec], "akka.tcp://agent@127.0.0.1:5151/user/server"))

  val ds = FileSource("data/iislog/SZ_test/", Option("iis"), None, Some("utf-8"), Some(LineCodec()), None, 0)

  @Test
  def testCreate() = {
    val config = Config("0","testSingle",
      SingleChannel(ds, Some(Regex("%{IIS_LOG}", Array(AddFields(Map(("test", "124"))))))),
      collector = Some(Collector("0", "localhost", 5151)), writers = List(ES2Writer("1", "yzh_es", Array(("yzh", 9200)), "logs_%{yyyyMMdd}", "iis", cache = 1000)))
    assert(Await.result(remote ? config, timeout.duration).asInstanceOf[Boolean])
  }

  @Test
  def testStop(): Unit = {
    assert(Await.result(remote ? (ds, START), timeout.duration).asInstanceOf[Boolean])
  }

  @Test
  def testStart(): Unit = {
    assert(Await.result(remote ? (ds, STOP), timeout.duration).asInstanceOf[Boolean])
  }

  @After
  def clear(): Unit = {
    system.terminate()
  }
}

class Exec(path: String) extends Actor {

  init()
  var remote: ActorRef = _

  def init(): Unit = context.actorSelection(path) ! Identify(path)

  def send(message: AnyRef): Boolean = {
    if (remote != null) {
      remote ! message
      true
    } else {
      TimeUnit.SECONDS.sleep(1)
      send(message)
    }
  }

  def receive = {
    case ActorIdentity(`path`, Some(actor)) =>
      println(s"Remote actor able: $path")
      remote = actor

    case ActorIdentity(`path`, None) => println(s"Remote actor not available: $path")
    case ReceiveTimeout => init()
    case message: AnyRef =>
      sender() ! send(message)
  }

}

