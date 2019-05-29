package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.RemoveFields
import com.haima.sage.bigdata.etl.filter.RuleProcessor

class RemoveFieldsProcessor(override val filter: RemoveFields) extends RuleProcessor[RichMap, RichMap, RemoveFields] {

  import com.haima.sage.bigdata.etl.common.Implicits._


  override def process(event: RichMap): RichMap = {
    /*
    * event.--(filter.fields.map {
      case Brackets(start, key, end) =>
        start + (if (event.contains(key)) key else "") + end
      case key =>
        key
    }.toSeq)*/
    event.--(filter.fields)
  }
}