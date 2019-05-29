package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Ignore
import com.haima.sage.bigdata.etl.filter.RuleProcessor


class IgnoreProcessor(override val filter: Ignore) extends RuleProcessor[RichMap, RichMap, Ignore] {

  override def process(event: RichMap):  RichMap = event

}