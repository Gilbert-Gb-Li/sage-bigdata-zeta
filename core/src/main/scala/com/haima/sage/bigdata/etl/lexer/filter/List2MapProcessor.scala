package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.List2Map
import com.haima.sage.bigdata.etl.filter.RuleProcessor

import scala.collection.mutable.ArrayBuffer

/**
  * 字段值替换
  *
  * @param filter
  */
class List2MapProcessor(override val filter: List2Map) extends RuleProcessor[RichMap, List[RichMap], List2Map] {


  override def process(event: RichMap): List[RichMap] = {
    event.get(filter.field) match {
      case Some(v: List[_]) =>
        List(event + (filter.field -> filter.fields.split(",").zip(v).toMap))
      case Some(v: Array[_]) =>
        List(event + (filter.field -> filter.fields.split(",").zip(v).toMap))
      case Some(v: ArrayBuffer[_]) =>
        List(event + (filter.field -> filter.fields.split(",").zip(v).toMap))
      case _ =>
        List(event)
    }
  }


}