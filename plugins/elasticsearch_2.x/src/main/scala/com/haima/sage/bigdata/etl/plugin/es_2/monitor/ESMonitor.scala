package com.haima.sage.bigdata.etl.plugin.es_2.monitor

import java.util.Date

import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.monitor.Monitor
import com.haima.sage.bigdata.etl.plugin.es_2.reader.ESLogReader

import scala.concurrent.Await

/**
  * Created by zhhuiyan on 14/12/27.
  */

class ESMonitor(conf: ES2Source, parser: Parser[MapRule]) extends Monitor {

  implicit var id: String = _

  @throws[Exception]
  override def run():Unit = {
    try {
      val value: Any = try {
        import akka.pattern.ask

        import scala.concurrent.duration._
        implicit val timeout = Timeout(5 minutes)
        val position = Await.result(positionServer ? (Opt.GET, s"$id:"+conf.uri), timeout.duration).asInstanceOf[ReadPosition]
        if (position.position == 0) {
          conf.start
        } else {
          conf.start match {
            case date: Date =>
              new Date(position.position)
            case _ =>
              position.position
          }

        }
      } catch {
        case e: Exception =>
          conf.start
      }
      val step = if (conf.step <= 0) 1000 else conf.step
      send(Some(new ESLogReader(conf, value, step,id)), Some(Parser(filter = parser.filter, metadata = parser.metadata)))

    } catch {
      case e: Exception =>

        logger.error("create reader error:{}", e)
        context.parent ! (ProcessModel.MONITOR, Status.ERROR,s"MONITOR_ERROR:${e.getMessage}")
        context.stop(self)
    }
  }

  override def close(): Unit = {
    //TODO some thing need

  }

  override def receive: Receive = {
    //获取数据通道的ID,用于position,sender() 是process
    case id: String =>
      this.id = id
    case obj =>
      super.receive(obj)
  }
}
