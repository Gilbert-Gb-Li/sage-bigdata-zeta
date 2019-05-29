package com.haima.sage.bigdata.etl.monitor

import java.text.SimpleDateFormat
import java.util.Date

import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.reader.JDBCLogReader

import scala.concurrent.Await

/**
  * Created by zhhuiyan on 14/12/27.
  */

class JDBCMonitor(conf: JDBCSource, parser: Parser[MapRule]) extends Monitor {
  private final val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

  implicit var id: String = _

  @throws[Exception]
  override def run(): Unit = {
    import akka.pattern.ask

    import scala.concurrent.duration._
    implicit val timeout = Timeout(5 minutes)
    //获取position
    val position= Await.result(positionServer ? (Opt.GET, s"$id:" + conf.uri), timeout.duration).asInstanceOf[ReadPosition]
    //判断是否重新加载
    val fromStart: String = conf.get("fromstart", null)
    try {
      val value: Long = try {
        if (position.position == 0 || "true".equals(fromStart)) {
          conf.start match {
            case data: Int =>
              data.toLong
            case data: Long =>
              data
            case data: Date =>
              data.getTime
            case data: BigDecimal =>
              data.longValue()
            case data: String =>
              if (data.matches("\\d+")) {
                data.toLong
              } else {
                format.parse(data).getTime
              }
          }
        } else {
          position.position
        }
      } catch {
        case e: Exception =>
          logger.error(s"you set start index must be java.util.long or format date with yyyy-MM-dd HH:mm:ss.S", e)
          0
      }
      val step = if (conf.step <= 0) 1000 else conf.step //定义步长
      logger.debug(s"jdbc monitor start:$value,step:$step")
      val dataSourceConf = conf.copy(start = value, step = step)
      val reader = new JDBCLogReader(dataSourceConf,position)
      send(Some(reader), Some(TransferParser(filter = parser.filter, metadata = parser.metadata)))
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
