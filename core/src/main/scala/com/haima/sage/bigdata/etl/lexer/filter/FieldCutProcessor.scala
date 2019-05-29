package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.FieldCut
import com.haima.sage.bigdata.etl.filter.RuleProcessor

class FieldCutProcessor(override val filter: FieldCut) extends RuleProcessor[RichMap, RichMap, FieldCut] {

  def realProcess(value: String): String = {

    val (f, t) = if (filter.from < 0) {
      if (filter.limit < value.length) {
        (0, value.length - filter.limit)
      } else {
        (0, value.length)
      }
    } else {
      if (filter.from > value.length) {
        (0, value.length)
      } else if (filter.from + filter.limit > value.length) {
        (filter.from, value.length)
      } else {
        (filter.from, filter.from + filter.limit)
      }
    }
    value.substring(f, t)
    //    (event - filter.field) + (filter.field ->
    //      event.getOrElse(filter.field, "").toString.substring(f, t))
  }

  override def process(event: RichMap): RichMap =
    filter.get(event) match {
      case Some(value: String) if value != null && value.length > 0 =>
        event + (filter.field -> realProcess(value))
      case _ =>
        event
    }

}