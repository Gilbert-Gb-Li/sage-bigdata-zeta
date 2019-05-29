package com.haima.sage.bigdata.etl.base
import akka.pattern._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants.CONF
import org.junit.{After, Test}

import scala.concurrent.{Await, Future}

class ActorTest {
  import scala.concurrent.duration._
  implicit val timeout = Timeout(1 minutes)
  final val system = ActorSystem("worker", CONF.resolve())

  @Test
  def run(): Unit ={
   implicit val ref=system.actorOf(Props[RunActor])
    assert(get[Int](123).startsWith("Int"))
    assert(get[String]("123").startsWith("String"))
    assert(get[List[_]](List()).startsWith("Any"))
  }
  @After
  def close(): Unit ={
    system.terminate()
  }

  def get[T](d:T)(implicit ref:ActorRef):String={
    Await.result[String]((ref ? d).asInstanceOf[Future[String]],timeout.duration)
  }
}



trait ActorInt extends Actor {
  def execI: Receive = {
    case data: Int =>
      sender() ! s"Int($data)"
  }
}

trait ActorString extends Actor {
  def execS: Receive = {
    case data: String =>
      sender() ! s"String($data)"
  }
}

trait ActorDefault extends Actor {

  def execD: Receive = {
    case data =>
      sender() ! s"Any($data)"
  }

}

class RunActor extends ActorInt with ActorString with ActorDefault {
  override def receive: Receive = execI.orElse(execS).orElse(execD)
}
