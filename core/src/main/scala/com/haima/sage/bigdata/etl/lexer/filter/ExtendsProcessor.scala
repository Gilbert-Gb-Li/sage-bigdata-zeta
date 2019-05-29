package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Extends
import com.haima.sage.bigdata.etl.filter.RuleProcessor

import scala.collection.mutable.ArrayBuffer

/**
  * 字段值替换
  *
  * @param filter
  */
class ExtendsProcessor(override val filter: Extends) extends RuleProcessor[RichMap, List[RichMap], Extends] {


  override def process(event: RichMap): List[RichMap] ={
    event.get(filter.field) match {
      case Some(v: List[_]) =>
        v.map {
          case t: RichMap => t ++ (event - filter.field)
          case t: Map[String@unchecked, _] => RichMap(t ++ (event - filter.field))
          case t =>
            event + (filter.field -> t)

        }
      case Some(v: Array[_]) =>
        v.map {
          case t: RichMap => t ++ (event - filter.field)
          case t: Map[String@unchecked, _] => RichMap(t ++ (event - filter.field))
          case t =>
            event + (filter.field -> t)
        }.toList
      case Some(v: ArrayBuffer[_]) =>
        v.map {
          case t: RichMap => t ++ (event - filter.field)
          case t: Map[String@unchecked, _] => RichMap(t ++ (event - filter.field))
          case t =>
            event + (filter.field -> t)
        }.toList
      case _ =>
        List(event)
    }
  }


}