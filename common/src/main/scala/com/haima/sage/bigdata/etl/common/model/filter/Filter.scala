package com.haima.sage.bigdata.etl.common.model.filter

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.filter.RuleProcessor

import scala.annotation.tailrec

case class Filter(rules: Array[MapRule] = Array()) extends Serializable {

  private lazy val processors: Array[RuleProcessor[RichMap, _, _ <: MapRule]] = rules.map(r => {
    Class.forName(Constants.CONF.getString("app.lexer.filter." + r.name)).getConstructor(r.getClass).newInstance(r).asInstanceOf[RuleProcessor[RichMap, RichMap, _ <: MapRule]]
  })


  override def toString: String = s"Filter(rules:Array(${
    rules.map(_.toString).mkString(",")
  }}))"


  private def handle(processor: RuleProcessor[RichMap, _, _], event: RichMap): List[RichMap@unchecked] = {
    processor.process(event) match {
      case t: RichMap =>
        List(t)
      case t: List[RichMap@unchecked] =>
        t
    }
  }

  @tailrec
  private def parse(rules: List[RuleProcessor[RichMap, _, _]])(events: List[RichMap]): List[RichMap] = {

    rules match {
      case Nil =>
        events
      case head :: tail =>
        val rt = events.flatMap(t => handle(head, t))
        parse(tail)(rt)

    }

  }

  def filter(event: RichMap): List[RichMap] = {
    parse(processors.toList)(List(event))

  }

  def filter(rules: List[RuleProcessor[RichMap, _, _]], events: List[RichMap]): List[RichMap] = {
    parse(rules)(events)
  }
}


