package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Drop
import com.haima.sage.bigdata.etl.filter.RuleProcessor

class DropProcessor(override val filter: Drop) extends RuleProcessor[RichMap, RichMap, Drop] {


  override def process(event: RichMap): RichMap = event.empty

}