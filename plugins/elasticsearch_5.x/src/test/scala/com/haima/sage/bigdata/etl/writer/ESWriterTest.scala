package com.haima.sage.bigdata.etl.writer

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, DeadLetter, Props}
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.haima.sage.bigdata.etl.common.model.{ES5Writer, Opt, Status, Writer}
import com.haima.sage.bigdata.etl.plugin.es_5.writer.ElasticSearchWriter
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

import scala.concurrent.duration._

/**
  * Created by zhhuiyan on 15/2/6.
  */
class ESWriterTest extends Mapper {
  implicit val timeout = Timeout(50 seconds)

  @Test
  def config(): Unit = {
    val writer = ES5Writer("0", "yzh_es", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1000)
    val json = mapper.writeValueAsString(writer)
    println(json)
    val data: Writer = mapper.readValue[Writer](json)
    println(data)
  }

  @Test
  def test(): Unit = {
    val system = ActorSystem("local")
    val metric: MetricRegistry = new MetricRegistry()
    val writer = system.actorOf(Props(classOf[ElasticSearchWriter], ES5Writer("0", "elasticsearch", Array(("127.0.0.1",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1000), metric.timer("test")), "startActor")

    val deadLetterListener = system.actorOf(Props[MyCustomDeadLetterListener])
    system.eventStream.subscribe(deadLetterListener, classOf[DeadLetter])
    writer ! Map[String, Any](
      "test" -> "1",
      "test2" -> "2",
      "test" -> "3",
      "test" -> "4"
    )
    writer ! Opt.STOP
    while (MailBox.flag) {
      TimeUnit.SECONDS.sleep(1)
    }

  }


}

object MailBox {
  var flag = true
}

class MyCustomDeadLetterListener extends Actor {
  def receive = {
    case DeadLetter(msg: Status.Status, _, _) =>
      MailBox.flag = false
    case DeadLetter(msg, _, _) =>

      println(s"FROM CUSTOM LISTENER $msg")

  }
}

