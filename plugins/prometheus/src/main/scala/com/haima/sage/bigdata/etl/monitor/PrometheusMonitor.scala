package com.haima.sage.bigdata.etl.monitor

import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.reader.PrometheusReader

import scala.concurrent.Await
import scala.language.postfixOps

/**
  * Created by liyju on 2017/8/17.
  */
class PrometheusMonitor (conf: PrometheusSource, parser: Parser[MapRule]) extends Monitor {


  @throws[Exception]
  override def run(): Unit = {

    val fromStart: String = conf.get("fromstart", null)
    try {
      val value: Long =  {
        import akka.pattern.ask
        import scala.concurrent.duration._
        implicit val timeout = Timeout(5 minutes)
        val position = Await.result(positionServer ? (Opt.GET, conf.uri), timeout.duration).asInstanceOf[ReadPosition]
        if (position.position == 0 || "true".equals(fromStart)) {
          conf.start
        } else {
          position.position
        }
      }
      val step = conf.step
      send(Some(new PrometheusReader(conf.copy(start = value, step = step))), Some(TransferParser(filter = parser.filter, metadata = parser.metadata)))
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
}
