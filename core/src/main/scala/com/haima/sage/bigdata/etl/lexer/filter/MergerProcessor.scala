package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Merger
import com.haima.sage.bigdata.etl.filter.RuleProcessor

class MergerProcessor(override val filter: Merger) extends RuleProcessor[RichMap, RichMap, Merger] {
  override def process(event: RichMap): RichMap = {

    val value = filter.fields.map(field => event.get(field).getOrElse("").toString).mkString(filter.sep.getOrElse(""))

    event + (filter.field -> value)
  }
}