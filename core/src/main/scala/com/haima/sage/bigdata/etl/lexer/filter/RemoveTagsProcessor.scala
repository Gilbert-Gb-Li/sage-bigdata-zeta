package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model.filter.RemoveTags
import com.haima.sage.bigdata.etl.filter.RuleProcessor

class RemoveTagsProcessor(override val filter: RemoveTags) extends RuleProcessor[RichMap, RichMap, RemoveTags] {

  import com.haima.sage.bigdata.etl.common.Brackets

  override def process(event: RichMap):  RichMap = {
//    event -- filter.fields.map {
//      case Brackets(start, key, end) =>
//        start + (if (event.contains(key)) key else "") + end
//      case key =>
//        key
//    }

    val fields = filter.fields.map {
      case Brackets(start, key, end) =>
        start + (if (event.contains(key)) key else "") + end
      case key =>
        key
    }
    event -- fields

  }
}