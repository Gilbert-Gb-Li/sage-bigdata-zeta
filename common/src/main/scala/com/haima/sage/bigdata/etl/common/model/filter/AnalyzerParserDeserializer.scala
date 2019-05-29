package com.haima.sage.bigdata.etl.common.model.filter

import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.Parser
import com.haima.sage.bigdata.etl.store.resolver.ParserStore

class AnalyzerParserDeserializer(val vc: Class[AnalyzerParser]) extends StdDeserializer[AnalyzerParser](vc) {

  private lazy val store = Class.forName(Constants.getApiServerConf(Constants.STORE_PARSER_CLASS)).asInstanceOf[Class[ParserStore]].newInstance()

  @throws[JsonProcessingException]
  def deserialize(jsonparser: JsonParser, context: DeserializationContext): AnalyzerParser = {
    val data = jsonparser.readValueAs[Map[String, Any]](classOf[Map[String, Any]])

    val parser: Option[Parser[MapRule]] =
      data.get("ref") match {
        case Some(ref: String) =>
          store.get(ref) match {
            case Some(parser) =>
              parser.parser
            case _ =>
              data.get("parser") match {
                case Some(a: Parser[MapRule@unchecked]) =>
                  Some(a)
                case _ =>
                  None

              }
          }
        case _ =>
          data.get("parser") match {
            case Some(a: Parser[MapRule@unchecked]) =>
              Some(a)
            case _ =>
              None

          }

      }
    AnalyzerParser(parser, data.get("ref").map(_.toString))
  }
}