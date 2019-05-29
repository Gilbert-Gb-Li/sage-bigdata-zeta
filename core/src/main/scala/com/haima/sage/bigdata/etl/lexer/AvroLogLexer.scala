package com.haima.sage.bigdata.etl.lexer

import akka.actor.ActorRef
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{ObjectReader, SerializationFeature}
import com.fasterxml.jackson.dataformat.avro.{AvroMapper, AvroSchema}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.base
import com.haima.sage.bigdata.etl.common.exception.LogParserException
import com.haima.sage.bigdata.etl.common.model.{AvroParser, RichMap}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.apache.avro.Schema
import org.slf4j.{Logger, LoggerFactory}


class AvroLogLexer(override val parser: AvroParser, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {
  val lexer = AvroLogLexer(parser)
}

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/23 15:27.
  * 默认按照String 解析一行日志到，json
  */
object AvroLogLexer {
  val logger: Logger = LoggerFactory.getLogger(classOf[JSONLogLexer])

  case class Lexer(parser: AvroParser) extends base.Lexer[String, RichMap] {
    private var encoding: String = _
    lazy val mapper: ObjectReader = {
      val schema = new AvroSchema(new Schema.Parser().setValidate(true).parse(parser.schema))
      val mapper = new AvroMapper() with ScalaObjectMapper
      mapper.setVisibility(PropertyAccessor.FIELD, Visibility.PUBLIC_ONLY)
      // mapper.registerModule(DefaultScalaModule).setSerializationInclusion(Include.NON_NULL)
      mapper.registerModule(DefaultScalaModule).setSerializationInclusion(Include.NON_ABSENT)
      mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      mapper.readerFor(classOf[Map[String, Any]]).`with`(schema)
    }


    def setTemplet(encoding: String) {
      this.encoding = encoding
    }


    @throws(classOf[LogParserException])
    def parse(_event: String): RichMap = {
      try {
        //, Feature.DisableCircularReferenceDetect
        val event = _event.trim
        mapper.readValue[Map[String, Any]](event)
      } catch {
        case e: Exception =>
          throw new LogParserException(s" parse avro[${parser.schema}] error", e)
      }


    }
  }

  def apply(parser: AvroParser): Lexer = {
    Lexer(parser)
  }
}