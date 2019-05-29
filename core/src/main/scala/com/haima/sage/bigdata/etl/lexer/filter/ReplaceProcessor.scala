package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.Brackets
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Replace
import com.haima.sage.bigdata.etl.filter.RuleProcessor

/**
  * 字段值替换
  *
  * @param filter
  */
class ReplaceProcessor(override val filter: Replace) extends RuleProcessor[RichMap, RichMap, Replace] {


  override def process(event: RichMap): RichMap =
    event.get(filter.field) match {
      case Some(v) if v == null || v.toString.trim.length > 0 =>
        val _to = filter.to match {
          case null=>
            ""
          case Brackets(start, key, end) =>
            event.get(key) match {
              case Some(v2) if v2 != null =>
                start + v2 + end
              case _ =>
                start + end
            }
          case d =>
            d

        }
        event + (filter.field -> v.toString.replaceAll(filter.from, _to))
      case _ =>
        event
    }

}