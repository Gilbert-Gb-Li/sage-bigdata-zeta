package com.haima.sage.bigdata.etl.common.model.filter

import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.Analyzer
import com.haima.sage.bigdata.etl.store.AnalyzerStore

class ReAnalyzerDeserializer(val vc: Class[ReAnalyzer]) extends StdDeserializer[ReAnalyzer](vc) {

  private lazy val store = Class.forName(Constants.getApiServerConf(Constants.STORE_ANALYZER_CLASS)).asInstanceOf[Class[AnalyzerStore]].newInstance()

  @throws[JsonProcessingException]
  def deserialize(parser: JsonParser, context: DeserializationContext): ReAnalyzer = {
    val data = parser.readValueAs[Map[String, Any]](classOf[Map[String, Any]])

    val analyzer: Option[Analyzer] =
      data.get("ref") match {
        case Some(ref: String) =>
          store.get(ref) match {
            case Some(p) =>
              p.data
            case _ =>
              data.get("analyzer") match {

                case Some(a: Analyzer) =>
                  Some(a)
                case _ =>
                  None
              }
          }
        case _ =>
          data("analyzer") match {
            case Some(a: Analyzer) =>
              Some(a)
            case _ =>
              None
          }

      }

    ReAnalyzer(analyzer, ref = data.get("ref").map(_.toString))
  }
}