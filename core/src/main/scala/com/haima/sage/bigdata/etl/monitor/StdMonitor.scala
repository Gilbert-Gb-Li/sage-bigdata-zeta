package com.haima.sage.bigdata.etl.monitor

import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model.{DataSource, Parser, ProcessModel, Status}
import com.haima.sage.bigdata.etl.reader.StdLogReader


class StdMonitor(conf: DataSource, parser: Parser[MapRule]) extends Monitor {


  def reader(): Option[LogReader[_]] = Some(new StdLogReader(conf))

  @throws[Exception]
  override def run():Unit = {
    try {
      send(reader(), Some(parser))
    } catch {
      case e: Exception =>
        logger.error("create reader error:{}", e)
        context.parent !(ProcessModel.MONITOR, Status.ERROR,s"MONITOR_ERROR:${e.getMessage}")
        context.stop(self)
    }

    }

  override def close(): Unit = {

  }


}
