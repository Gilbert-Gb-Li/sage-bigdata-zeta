package com.haima.sage.bigdata.etl.plugin.es_6.monitor

import java.text.SimpleDateFormat
import java.util.Date

import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.monitor.Monitor
import com.haima.sage.bigdata.etl.plugin.es_6.reader.ESLogReader

import scala.concurrent.Await

/**
  * Created by zhhuiyan on 14/12/27.
  */

class ESMonitor(conf: ES6Source, parser: Parser[MapRule]) extends Monitor {
  private final val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
  implicit var id: String = _
  @throws[Exception]
  override def run():Unit = {
    try {


      import scala.concurrent.duration._
      implicit val timeout = Timeout(5 minutes)
      val step = if (conf.step <= 0) 1000 else conf.step

      val position = Await.result(positionServer ? (Opt.GET,s"$id:" + conf.uri), timeout.duration).asInstanceOf[ReadPosition]

      send(Some(new ESLogReader(conf,if (position.position == 0) {
        conf.start match {
          case data: String  if data.matches("\\d+") =>
            data.toLong
          case data: String =>
            format.parse(data)

        }
      } else {
        conf.start match {
          case data: String  if ! data.matches("\\d+") =>
            new Date(position.position)
          case _ =>
            position.position

        }


      } , step,id)), Some(NothingParser(filter = parser.filter, metadata = parser.metadata)))
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
