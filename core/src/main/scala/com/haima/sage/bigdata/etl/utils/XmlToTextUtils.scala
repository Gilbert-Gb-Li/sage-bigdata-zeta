package com.haima.sage.bigdata.etl.utils

import java.io.ByteArrayInputStream

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.dataformat.xml.XmlFactory
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.haima.sage.bigdata.etl.common.model.RichMap
import javax.xml.stream.{XMLInputFactory, XMLStreamReader}

import scala.collection.mutable.ListBuffer
import scala.util.Try

object XmlToTextUtils {
  def xmlToText(event: RichMap,
                attrs: Seq[String] = List("text", "shared-link"),
                field: String = "raw", dstField: String = "raw_text"): RichMap = {
    Try {
      val value: String = event.get(field) match {
        case Some(x: Any) =>
          x.toString
        case None => null
      }
      if (value == null) {
        return event
      }
      val parser = readXml(value)
      val data = parse(parser, attrs)
      event + (dstField -> data)
    }.getOrElse(event)
  }

  private def parse(parser: FromXmlParser, attrs: Seq[String]): String = Try {
    val dst = new ListBuffer[String]
    var flag = true
    while (flag) {
      val token = parser.nextToken()
      flag = parser.hasCurrentToken
      if (flag && token == JsonToken.VALUE_STRING) {
        val name = parser.getCurrentName
        if (attrs.exists(_.equals(name))) {
          val value = parser.getValueAsString
          if (value != null && value.nonEmpty) {
            dst += value
          }
        }
      }
    }
    dst.mkString("\n")
  }.getOrElse(null)

  private def readXml(value: String): FromXmlParser = {
    val factory: XmlFactory = {
      new XmlFactory()
    }
    val inputFactory: XMLInputFactory = {
      System.setProperty("javax.xml.stream.XMLInputFactory", "com.ctc.wstx.stax.WstxInputFactory")
      val inputFactory: XMLInputFactory = XMLInputFactory.newFactory
      inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
      inputFactory
    }
    val reader: XMLStreamReader = inputFactory.createXMLStreamReader(new ByteArrayInputStream(value.getBytes))
    implicit val parser: FromXmlParser = factory.createParser(reader)
    parser
  }
}
