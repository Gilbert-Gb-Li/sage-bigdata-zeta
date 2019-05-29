package com.haima.sage.bigdata.etl.monitor

import java.text.SimpleDateFormat
import java.util.Date

import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.reader.AWSReader

import scala.concurrent.Await


class AWSMonitor(conf: AWSSource, parser: Parser[MapRule]) extends Monitor {


  private final val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
  val value: Date = try {
    import akka.pattern.ask

    import scala.concurrent.duration._
    implicit val timeout = Timeout(5 minutes)
    val position = Await.result(positionServer ? (Opt.GET, conf.uri), timeout.duration).asInstanceOf[ReadPosition]
    if (position.position == 0) {
      conf.start match {
        case data: Int =>
          new Date(data.toLong)
        case data: Long =>
          new Date(data.toLong)
        case data: Date =>
          data
        case data: BigDecimal =>
          new Date(data.longValue())
        case data: String =>
          if (data.matches("\\d+")) {
            new Date(data.toLong)
          } else {
            format.parse(data)
          }
      }
    } else {
      new Date(position.position)
    }
  } catch {
    case e: Exception =>
      logger.error(s"you set start index must be java.util.long or format date with yyyyMMdd HH:mm:ss.S ")
      new Date()
  }

  @throws[Exception]
  override def run():Unit = {
    try {
      conf.servers.foreach(server => {
        send(Some(new AWSReader(conf, server, value)), Some(parser))
      })

    } catch {
      case e: Exception =>
        logger.error("create reader error:{}", e)
        context.parent ! (ProcessModel.MONITOR, Status.ERROR,s"MONITOR_ERROR:${e.getMessage}")
        context.stop(self)
    }

  }

  override def close(): Unit = {

  }


}
