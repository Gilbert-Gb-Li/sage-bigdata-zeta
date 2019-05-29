package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{PrometheusSource, ReadPosition}
import com.haima.sage.bigdata.etl.stream.PrometheusStream

class PrometheusReader(conf: PrometheusSource) extends LogReader[RichMap] with Position {
  override val stream = PrometheusStream(conf)
  override val iterator: Iterator[RichMap] = new Iterator[RichMap] {
    override def next(): RichMap = {
      val event = stream.next()
      if (event != null) {
        position.recordIncrement()
        position.setPosition(event("timestamp").asInstanceOf[Long]+conf.step)
      }
      event
    }
    override def hasNext: Boolean = stream.hasNext
  }

  def skip(skip: Long): Long = 0

  def path: String = conf.uri

  val position: ReadPosition = {
    ReadPosition(path, 0, conf.start)
  }

}

