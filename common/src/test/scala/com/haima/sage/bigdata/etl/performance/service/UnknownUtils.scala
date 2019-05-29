package com.haima.sage.bigdata.etl.performance.service

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable

object UnknownUtils {
  def process(event: RichMap): RichMap = {
    var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    dst ++= event
    dst += ("meta_table_name" -> "unknown")
    RichMap(dst.toMap)
  }
}
