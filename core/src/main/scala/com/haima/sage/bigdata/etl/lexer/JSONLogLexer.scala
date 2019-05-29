package com.haima.sage.bigdata.etl.lexer

import akka.actor.ActorRef
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common._
import com.haima.sage.bigdata.etl.common.exception.{CollectorException, LogParserException}
import com.haima.sage.bigdata.etl.common.model.{JsonParser, RichMap}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import com.haima.sage.bigdata.etl.utils.Mapper
import org.slf4j.{Logger, LoggerFactory}


/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/23 15:27.
  * 默认按照String 解析一行日志到，json
  */
object JSONLogLexer {
  val logger: Logger = LoggerFactory.getLogger(classOf[JSONLogLexer])

  case class Lexer(parser: JsonParser) extends base.Lexer[String, RichMap] {
    private var encoding: String = _
    lazy val mapper: ObjectMapper with ScalaObjectMapper = new Mapper() {}.mapper


    def setTemplet(encoding: String) {
      this.encoding = encoding
    }


    @throws(classOf[CollectorException])
    def parse(_event: String): RichMap = {
      try {
        //, Feature.DisableCircularReferenceDetect
        val event = _event.trim
        if (event.startsWith("{")) {

          mapper.readValue[Map[String, Any]](event)

        } else {
          Map("raw" -> mapper.readValue[List[Map[String, Any]]](event))
        }


      } catch {
        case e: Exception =>
          throw new LogParserException(s"json parse error :${e.getMessage}", e)
      }


    }
  }

  def apply(parser: JsonParser = JsonParser()): Lexer = {
    Lexer(parser)
  }
}

class JSONLogLexer(override val parser: JsonParser, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {
  val lexer = JSONLogLexer(parser)
}
