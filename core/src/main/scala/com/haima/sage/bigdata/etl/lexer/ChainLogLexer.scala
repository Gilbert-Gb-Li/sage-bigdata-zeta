package com.haima.sage.bigdata.etl.lexer


import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common._
import com.haima.sage.bigdata.etl.common.exception.CollectorException
import com.haima.sage.bigdata.etl.common.model.{Parser => MParser, _}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import com.haima.sage.bigdata.etl.utils.lexerFactory
import org.slf4j.{Logger, LoggerFactory}


/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/23 15:27.
  * 默认按照String 解析一行日志到，json
  */
object ChainLogLexer {
  private val logger: Logger = LoggerFactory.getLogger(classOf[SelectLogLexer])

  case class Lexer(parser: ChainParser) extends base.Lexer[String, RichMap] {
    val lexers: Array[Option[base.Lexer[String, RichMap]]] = parser.parsers.map(lexerFactory.instance)

    def parse(line: String, lexer: Option[base.Lexer[String, RichMap]]): Map[String, Any] = {
      lexer match {
        case Some(lx) =>
          lx.parse(line)
        case None =>
          Map()
      }
    }

    @throws(classOf[CollectorException])
    def parse(event: String): RichMap = {
      RichMap(lexers.map(parse(event, _)).reduce(_ ++ _))
    }
  }

  def apply(parser: ChainParser): Lexer = {
    Lexer(parser)
  }
}

class ChainLogLexer(override val parser: ChainParser, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {
  val lexer = ChainLogLexer(parser)
}
