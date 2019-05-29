package com.haima.sage.bigdata.etl.lexer

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.base
import com.haima.sage.bigdata.etl.common.exception.{CollectorException, LogParserException}
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model.{Event, Parser, RichMap, TransferParser}
import com.haima.sage.bigdata.etl.metrics.MeterReport

object Transfer {
  def apply(parser: Parser[MapRule]): base.Lexer[RichMap, RichMap] = new base.Lexer[RichMap, RichMap] {
    /**
      * parse take time is long??
      *
      * @throws LogParserException
      */
    @throws(classOf[LogParserException])
    override def parse(from: RichMap): RichMap = from
  }
}

class Transfer(override val parser:TransferParser, override val writers: List[ActorRef], override val report: MeterReport) extends DefaultLexer[Event] {
  /**
    * parse take time is long??
    *
    */
  override def parse(from: Event): RichMap = {
    RichMap(from.header.getOrElse(Map()) ++ Map[String, Any]("raw" -> from.content))
  }
}