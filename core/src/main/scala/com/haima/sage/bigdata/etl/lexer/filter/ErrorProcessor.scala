package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Error
import com.haima.sage.bigdata.etl.filter.RuleProcessor

class ErrorProcessor(override val filter: Error) extends RuleProcessor[RichMap, RichMap, Error] {

  override def process(event: RichMap):  RichMap = event + ("error" -> "user set not parser ,log to error")

}
