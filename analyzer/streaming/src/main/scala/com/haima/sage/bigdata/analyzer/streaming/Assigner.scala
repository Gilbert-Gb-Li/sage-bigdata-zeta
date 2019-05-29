package com.haima.sage.bigdata.analyzer.streaming

import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

case class Assigner(field: String, maxOutOfOrderness: Long) extends Logger with AssignerWithPeriodicWatermarks[RichMap] {


  private var currentMaxTimestamp = 0L //最大处理过的日志的event time

  override def extractTimestamp(element: RichMap, previousElementTimestamp: Long): Long = {
    val timestamp: Long = element.get(field) match {
      case Some(d: Int) =>
        d.toLong
      case Some(d: Long) =>
        d
      case Some(d: Date) =>
        d.getTime
      case Some(d: Double) =>
        d.toLong
      case Some(d: Float) =>
        d.toLong
      case Some(d: Short) =>
        d.toLong
      case d =>
        logger.warn(s"field[${field}] is $d,class[${d.getClass}] in data[$element]")
        currentMaxTimestamp
    }
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    //return the watermark as current time minus the maximum time lag
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }


}
