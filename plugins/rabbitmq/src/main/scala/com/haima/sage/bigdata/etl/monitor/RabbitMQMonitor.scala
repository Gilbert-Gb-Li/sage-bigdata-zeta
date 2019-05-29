package com.haima.sage.bigdata.etl.monitor

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.reader.RabbitMQBatchReader


class RabbitMQMonitor(conf: RabbitMQSource, parser: Parser[MapRule]) extends Monitor {

  @throws[Exception]
  override def run():Unit = {

    try {
      send(Some(new RabbitMQBatchReader(conf)), Some(parser))
    } catch {
      case e: Exception =>
        logger.error(s"create RabbitMQ source error:$e")
        context.parent ! (ProcessModel.MONITOR, Status.ERROR,s"MONITOR_ERROR:${e.getMessage}")
        context.stop(self)
    }
  }

  override def close(): Unit = {

  }


}
