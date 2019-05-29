package com.haima.sage.bigdata.etl.common.model.filter

import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.Parser
import com.haima.sage.bigdata.etl.store.resolver.ParserStore

class ReParserDeserializer(val vc: Class[ReParser]) extends StdDeserializer[ReParser](vc) {

  private lazy val store = Class.forName(Constants.getApiServerConf(Constants.STORE_PARSER_CLASS)).asInstanceOf[Class[ParserStore]].newInstance()

  @throws[JsonProcessingException]
  def deserialize(jsonParser: JsonParser, context: DeserializationContext): ReParser = {
    val data = jsonParser.readValueAs[Map[String, Any]](classOf[Map[String, Any]])

    val parser: Option[Parser[MapRule]] =
      data.get("ref") match {
        case Some(ref: String) =>
          store.get(ref) match {
            case Some(p) =>
              p.parser
            case _ =>
              data.get("parser") match {
                case Some(a: Parser[MapRule@unchecked]) =>
                  Option(a)
                case _ =>
                  None

              }
          }
        case _ =>
          data.get("parser") match {
            case Some(a: Parser[MapRule@unchecked]) =>
              Option(a)
            case _ =>
              None

          }

      }
    ReParser(data.get("field").map(_.toString), parser.orNull, data.get("ref").map(_.toString))
  }
}