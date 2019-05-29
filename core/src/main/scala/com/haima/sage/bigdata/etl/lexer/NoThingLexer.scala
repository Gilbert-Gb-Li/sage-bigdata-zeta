package com.haima.sage.bigdata.etl.lexer

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model.NothingParser
import com.haima.sage.bigdata.etl.common.base
import com.haima.sage.bigdata.etl.common.exception.CollectorException
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.slf4j.{Logger, LoggerFactory}


object NoThingLexer {
  val logger: Logger = LoggerFactory.getLogger(classOf[NoThingLexer])

  case class Lexer(parser: NothingParser) extends base.Lexer[String, RichMap] {
    @throws(classOf[CollectorException])
    def parse(from: String): RichMap = {
      Map("raw" -> from)
    }
  }

  def apply(parser: NothingParser): Lexer = {
    Lexer(parser)
  }
}

class NoThingLexer(override val parser: NothingParser, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer   {

  val lexer = NoThingLexer(parser)
}