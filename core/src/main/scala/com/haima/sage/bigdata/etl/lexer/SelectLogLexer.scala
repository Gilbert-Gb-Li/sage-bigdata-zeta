package com.haima.sage.bigdata.etl.lexer


import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.base
import com.haima.sage.bigdata.etl.common.exception.CollectorException
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.common.model.filter.{Filter, MapRule}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import com.haima.sage.bigdata.etl.utils.lexerFactory
import org.slf4j.{Logger, LoggerFactory}


/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/23 15:27.
  * 默认按照String 解析一行日志到，json
  */
object SelectLogLexer {
  private val logger: Logger = LoggerFactory.getLogger(classOf[SelectLogLexer])

  case class Lexer(parser: SelectParser) extends base.Lexer[String, RichMap] {
    val lexers = parser.parsers.map(p => (lexerFactory.instance(p), p.filter))

    def parse(line: String, lexers: List[(Option[base.Lexer[String, RichMap]], Array[MapRule])]): RichMap = {
      lexers.toList match {
        case head :: tails =>
          parse(line, head, tails)
        case Nil =>
          Map("error" -> "your set all Parser can't parse the data, please recheck that")
      }
    }

    def parse(line: String, lexer: (Option[base.Lexer[String, RichMap]], Array[MapRule]), lexers: List[(Option[base.Lexer[String, RichMap]], Array[MapRule])]): RichMap = {
      lexer match {
        case (Some(lx), rules) if rules != null && rules.nonEmpty =>
          val map = lx.parse(line)
          if (map.isEmpty) {
            parse(line, lexers)
          } else {
            map.get("error") match {
              case None =>
/*
* fixme
* */
                Filter(rules).filter(map).head
              case Some(any) =>
                parse(line, lexers)
            }
          }
        case (Some(lx), _) =>
          val map = lx.parse(line)
          if (map.isEmpty) {
            parse(line, lexers)
          } else {
            map.get("error") match {
              case None =>
                map
              case Some(any) =>
                parse(line, lexers)
            }
          }
        case _ =>
          parse(line, lexers)

      }
    }

    @throws(classOf[CollectorException])
    def parse(event: String): RichMap = {
      parse(event, lexers.toList)

    }
  }

  def apply(parser: SelectParser): Lexer = {
    Lexer(parser)
  }


}

class SelectLogLexer(override val parser: SelectParser, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {
  val lexer: base.Lexer[String, RichMap] = SelectLogLexer(parser)
}
