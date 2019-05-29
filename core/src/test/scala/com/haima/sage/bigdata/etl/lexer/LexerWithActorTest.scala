package com.haima.sage.bigdata.etl.lexer


import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.filter.Mapping
import com.haima.sage.bigdata.etl.common.model.{Delimit, Event, Opt}
import com.haima.sage.bigdata.etl.metrics.{MeterReport, MetricRegistrySingle}
import org.junit.Test

import scala.concurrent.Await
import scala.concurrent.duration._

class LexerWithActorTest {
  Constants.init("worker.conf")

  final val system = ActorSystem("worker", CONF.resolve())
  val metric = MetricRegistrySingle.build()
  implicit val timeout = Timeout(5 minutes)
  final val operation_meter: MeterReport = MeterReport(Some(metric.meter(s"operation_success")), Some(metric.meter(s"operation_fail")), Some(metric.meter(s"operation_ignore")))
  var data: Map[String, Any] = null
  val writers = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case event: Map[String@unchecked, Any@unchecked] =>
        data = event


    }
  }))

  @Test
  def testDefaultParse(): Unit = {
    val lexer = system.actorOf(Props(classOf[DelimiterLexer], Delimit(fields = Array("a", "b", "c")), List(writers), operation_meter))
    //lexer ! (Opt.PUT, 100l, add_collector: Boolean, add_receive_time: Boolean, add_source: Boolean, add_raw: Boolean, add_path: Boolean, cpu_lower: Boolean)
    //添加所有补充信息
    Await.result(lexer ? (Opt.PUT, 100l, true, true, true, true, true, false), Duration.Inf)
    Await.result(lexer ? ("path11111", 1l, List(Event(Some(Map("size"->"100","length"->"10")), content = "1,2,3"))), Duration.Inf)
    println(data)
    assert(data.size==10)
    assert(data.contains("raw"))
    assert(data.contains("c@collector"))
    assert(data.contains("c@receive_time"))
    assert(data.contains("c@path"))
    assert(data.contains("c@source_size"))
    assert(data.contains("c@source_length"))
    //不添加补充信息
    Await.result(lexer ? (Opt.PUT, 100l, false, false, false, false, false, false), Duration.Inf)
    Await.result(lexer ? ("path11111", 1l, List(Event(content = "1,2,3"))), Duration.Inf)
    println(data)
    assert(data.size==4)

  }
  @Test
  def testRawRename(): Unit = {
    val lexer = system.actorOf(Props(classOf[DelimiterLexer], Delimit(fields = Array("a", "b", "c"),filter = Array(Mapping(Map("raw"->"message")))), List(writers), operation_meter))
    //lexer ! (Opt.PUT, 100l, add_collector: Boolean, add_receive_time: Boolean, add_source: Boolean, add_raw: Boolean, add_path: Boolean, cpu_lower: Boolean)
    //添加所有补充信息

    //不添加补充信息
    Await.result(lexer ? (Opt.PUT, 100l, false, false, false, false, false, false), Duration.Inf)
    Await.result(lexer ? ("path11111", 1l, List(Event(content = "1,2,3"))), Duration.Inf)
    println(data)
    assert(data.size==5)
    assert(data.contains("message"))
    assert(data.get("message").isDefined)

  }


}
