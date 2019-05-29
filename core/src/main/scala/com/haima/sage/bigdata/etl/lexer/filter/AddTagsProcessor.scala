package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.AddTags
import com.haima.sage.bigdata.etl.filter.RuleProcessor


class AddTagsProcessor(override val filter: AddTags) extends RuleProcessor[RichMap, RichMap, AddTags] {

  import com.haima.sage.bigdata.etl.common.Brackets
  import com.haima.sage.bigdata.etl.common.Implicits._

  override def process(event: RichMap): RichMap = {

    filter.fields.filterNot {
      case Brackets("", key, "") =>
        true
      case Brackets(_, "", _) =>
        true
      case Brackets(_, key, _) =>
        !event.contains(key)
      case _ =>
        true

    }.foldLeft(event) {
      case (e,Brackets(start, key, end)) =>
        e.get(key) match {
          case None =>
            e
          case Some(value) =>
            /*
            *map( a->"zhang san",b->2,c->3)   AddTags(set( s%{a}s)=>map( a->1,b->2,c->3,sas->"zhang san")
            * */
            e + ((start + key + end) -> value)
        }
    }

  }
}