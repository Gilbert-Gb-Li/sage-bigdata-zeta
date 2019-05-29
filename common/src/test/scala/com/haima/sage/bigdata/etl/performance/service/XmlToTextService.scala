package com.haima.sage.bigdata.etl.performance.service

import java.io.ByteArrayInputStream

import com.ctc.wstx.api.WstxInputProperties
import com.ctc.wstx.stax.WstxInputFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.dataformat.xml.XmlFactory
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.haima.sage.bigdata.etl.common.model.RichMap
import javax.xml.stream.{XMLInputFactory, XMLStreamReader}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class XmlToTextService(val field: String,
                       val attrs: Seq[XmlToTextConfig],
                       separator: String = "\n") {


  def process(event: RichMap): RichMap = Try {
    val value: String = event.get(field) match {
      case Some(x: Any) =>
        x.toString
      case None => null
    }
    if (value == null) {
      return event
    }
    val data = process(value)
    event + (field -> data)
  }.getOrElse(event)

  def process(value: String): String = {
    val parser = readXml(value)
    parse(parser)
  }

  private def parse(parser: FromXmlParser): String = Try {
    val dst = new ListBuffer[String]
    var flag = true
    while (flag) {
      val token: JsonToken = parser.nextToken()
      token match {
        case null | JsonToken.NOT_AVAILABLE =>
          flag = false
        case JsonToken.VALUE_STRING =>
          val name: String = parser.getCurrentName
          attrs.find(_.attr == name) match {
            case Some(item) =>
              var value = parser.getValueAsString
              if (value != null && value.nonEmpty) {
                value = value.replace("\n", "\\n")
                value = value.replace("\t", "\\t")
                if (item.withName) {
                  dst += item.attr + item.separator + value
                } else {
                  dst += value
                }
              }
            case _ =>
          }
        case _ =>
      }
    }
    dst.mkString(separator)
  } match {
    case Success(value) =>
      value
    case Failure(exception) =>
      exception.printStackTrace()
      null
  }

  val factory: XmlFactory = {
    val factory = new XmlFactory()
    factory
  }
  val inputFactory: XMLInputFactory = {
    val wstxInputFactory = new WstxInputFactory()
    wstxInputFactory.setProperty(WstxInputProperties.P_MAX_ATTRIBUTE_SIZE, 32000)
    wstxInputFactory.setProperty(WstxInputProperties.P_MAX_ELEMENT_DEPTH, 32000)
    wstxInputFactory.setProperty(WstxInputProperties.P_MAX_ENTITY_DEPTH, 32000)
    wstxInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
    wstxInputFactory
  }

  private def readXml(value: String): FromXmlParser = {
    val reader: XMLStreamReader = inputFactory.createXMLStreamReader(new ByteArrayInputStream(value.getBytes))
    implicit val parser: FromXmlParser = factory.createParser(reader)
    parser
  }
}

case class XmlToTextConfig(attr: String,
                           withName: Boolean = false,
                           separator: String = "=")
