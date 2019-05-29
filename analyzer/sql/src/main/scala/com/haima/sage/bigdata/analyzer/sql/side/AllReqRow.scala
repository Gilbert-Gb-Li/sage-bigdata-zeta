package com.haima.sage.bigdata.analyzer.sql.side

import java.util.concurrent.atomic.AtomicLong

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.types.Row

import scala.collection.mutable


trait AllReqRow extends RichFlatMapFunction[Row, Row] with ReqRow with Serializable {

  val cache: mutable.Map[String, mutable.ListBuffer[Map[String, Any]]] = mutable.Map()

  val cacheSize = new AtomicLong(0)

  def loadData(): Unit

  override def initCache(): Unit = {
    loadData()
  }

  def reloadCache(): Unit = {
    cache.clear()
    cacheSize.set(0)
    loadData()
  }
}
