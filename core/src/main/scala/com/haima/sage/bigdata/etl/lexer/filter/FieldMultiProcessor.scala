package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.FieldMulti
import com.haima.sage.bigdata.etl.filter.RuleProcessor

/**
  * 字段乘法
  *
  * @param filter
  */
class FieldMultiProcessor(override val filter: FieldMulti) extends RuleProcessor[RichMap, RichMap, FieldMulti] {


  override def process(event: RichMap): RichMap =
    event.get(filter.field) match {
      case Some(x: Int) =>
        event + (filter.field -> (x * filter.value))
      case Some(x: Long) =>
        event + (filter.field -> (x * filter.value))
      case Some(x: Double) =>
        event + (filter.field -> (x * filter.value))
      case Some(x: Float) =>
        event + (filter.field -> (x + filter.value))
      case Some(v) if v == null || v.toString.trim.length == 0 =>
        event + (filter.field -> 0)
      case Some(v) if v != null && v.toString.trim.matches("""(-)?\d+""") =>

        event + (filter.field -> (v.toString.trim.toLong * filter.value))
      case Some(v) if v != null && v.toString.trim.matches("""(-)?\d*\.?\d*""") =>
        event + (filter.field -> (v.toString.trim.toDouble * filter.value))

      case _ =>
        event
    }

}