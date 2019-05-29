package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.Brackets
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.AddFields
import com.haima.sage.bigdata.etl.filter.RuleProcessor

class AddFieldsProcessor(override val filter: AddFields) extends RuleProcessor[RichMap, RichMap, AddFields] {
  override def process(event: RichMap): RichMap = {

    filter.fields.filterNot {
      case ("", _) =>
        true
      case (_, "") =>
        true
      case (Brackets("", _, ""), _) =>
        /*
        *  map( a->"zhang san",b->2,c->3)   AddFields(map( %{a}->something)) nothing
        * */
        true
      case (Brackets(_, "", _), _) =>
        /*
       *  map( a->"zhang san",b->2,c->3)   AddFields(map( s%{}a->something)) nothing
       * */
        true
      case (Brackets(start, key, end), _) =>
        /*
      *  map( a1->"zhang san",b->2,c->3)   AddFields(map( a%{e}1->something)) nothing
      * */
        !event.contains(key) && event.contains(start + end)
      case (_, Brackets("", brackets, "")) =>
        /*
       *  map( a1->"zhang san",b->2,c->3)   AddFields(map( something->"%{e}")) nothing
       * */
        !event.contains(brackets)
      case _ =>
        false
    }.map {
      case (Brackets(start, key, end), Brackets(v1, value, v2)) =>



        /*
        * map( a->"zhang san",b->2,c->3)   AddFields(map( %{a}_1->"hello,%{a}"))=>map( a->1,b->2,c->3,a_1->"hello,zhang san")
        * map( a->"zhang san",b->2,c->3)   AddFields(map( sss%{e}->"hello,%{e}"))=>map( a->1,b->2,c->3,sss->"hello,")
        * map( a->"zhang san",b->2,c->3)   AddFields(map( sss%{a}->"hello,%{e}"))=>map( a->1,b->2,c->3,sssa->"hello,")
        * map( a->"zhang san",b->2,c->3)   AddFields(map( sss%{e}->"hello,%{a}"))=>map( a->1,b->2,c->3,sss->"hello,zhang san")
        * */
        event.get(value) match {
          case Some(v) =>
            (start + (if (event.contains(key)) key else "") + end, v1 + v + v2)
          case _ =>
            (start + (if (event.contains(key)) key else "") + end, v1 + v2)
        }

      case (Brackets(start, key, end), value) =>
        /*
        * map( a->"zhang san",b->2,c->3)   AddFields(map( sss%{a}->something))=>map( a->1,b->2,c->3,sssa->something)
        * map( a->"zhang san",b->2,c->3)   AddFields(map( sss%{e}->something))=>map( a->1,b->2,c->3,sss->something)
        * */
        (start + (if (event.contains(key)) key else "") + end, value)
      case (key, Brackets(v1, value, v2)) =>
        /*
        * map( a->"zhang san",b->2,c->3)   AddFields(map( sss->"hello,%{a}"))=>map( a->1,b->2,c->3,sss->"hello,zhang san")
        * map( a->"zhang san",b->2,c->3)   AddFields(map( sss->"hello,%{e}"))=>map( a->1,b->2,c->3,sss->"hello,")
        * */
        event.get(value) match {
          case Some(v) =>
            // set()
            (key, v1 + v + v2)
          case _ =>
            (key, v1 + v2)
        }

      case (key, value) =>
        (key, value)
    }.foldLeft(event) {
      case (e, (key, value)) =>
        e.+(key -> value)
    }
  }
}