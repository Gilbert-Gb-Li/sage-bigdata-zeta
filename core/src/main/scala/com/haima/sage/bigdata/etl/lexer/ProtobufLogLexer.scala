package com.haima.sage.bigdata.etl.lexer

import akka.actor.ActorRef
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{ObjectMapper, ObjectReader, SerializationFeature}
import com.fasterxml.jackson.dataformat.protobuf.ProtobufFactory
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchemaLoader
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.base
import com.haima.sage.bigdata.etl.common.exception.LogParserException
import com.haima.sage.bigdata.etl.common.model.{ProtobufParser, RichMap}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.slf4j.{Logger, LoggerFactory}


class ProtobufLogLexer(override val parser: ProtobufParser, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {
  val lexer = ProtobufLogLexer(parser)
}

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/23 15:27.
  * 默认按照String 解析一行日志到，json
  */
object ProtobufLogLexer {
  val logger: Logger = LoggerFactory.getLogger(classOf[JSONLogLexer])

  case class Lexer(parser: ProtobufParser) extends base.Lexer[String, RichMap] {
    private var encoding: String = _
    lazy val mapper: ObjectReader = {
      val schema = ProtobufSchemaLoader.std.parse(parser.schema)
      val mapper = new ObjectMapper(new ProtobufFactory) with ScalaObjectMapper
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


        val event = _event


        val data = mapper.readValue[Map[String, Any]](event.getBytes("ISO-8859-1"))
        data
      } catch {
        case e: Exception =>
          throw new LogParserException(s"protobuff[${parser.schema}] parse error", e)
      }


    }
  }

  def apply(parser: ProtobufParser): Lexer = {
    Lexer(parser)
  }
}