package com.haima.sage.bigdata.etl.lexer

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.base
import com.haima.sage.bigdata.etl.common.exception.LogParserException
import com.haima.sage.bigdata.etl.common.model.{Regex, RichMap}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import io.thekraken.grok.api.Grok
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/23 15:27.
  * 默认按照String 解析一行日志到，json
  */

class RegexLexer(override val parser: Regex, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {

  override lazy val lexer = RegexLexer(parser)
}

object RegexLexer {


  case class Lexer(value: String) extends base.Lexer[String, RichMap] {
    private lazy val grok: Grok = Grok.getInstance(value)

    def parse(event: String): RichMap = {
      val matched = grok.`match`(event)
      matched.captures(true)
      val log = matched.toMap
      // logger.debug(s"log---:$event")
      if (log == null || log.isEmpty) {
        throw new LogParserException("this data can`t parse by give regex:" + grok.getOriginalPattern)
      } else {
        RichMap(log.toMap).complex()
      }

    }
  }

  def apply(parser: Regex): Lexer = {
    Lexer(parser.value)
  }

  val logger: Logger = LoggerFactory.getLogger(classOf[RegexLexer])

  def main(args: Array[String]) {
    val grok = RegexLexer(Regex( """id=(?%{id:DATA}) time="(?%{logdate:DATA})" fw=(?%{fw:DATA})\bpri=(?%{pri:DATA}) type=(?%{type:DATA}) user=(?%{user:DATA}) src=(?%{src:IP}) op="(?%{op:DATA})" result=(?%{result:DATA}) recorder=(?%{recorder:DATA}) msg="(?%{msg:DATA})""""))
    logger.info(grok.parse( """id=tos time="2015-03-25 14:36:50" fw=中文 pri=6 type=system user=superman src=127.0.0.1 op="logout" result=0 recorder=AUTH msg=""""").toString())
  }

}
