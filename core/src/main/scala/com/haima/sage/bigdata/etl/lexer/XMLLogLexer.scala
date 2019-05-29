package com.haima.sage.bigdata.etl.lexer

import java.io.ByteArrayInputStream
import javax.xml.stream.{XMLInputFactory, XMLStreamReader}

import akka.actor.ActorRef
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.dataformat.xml.XmlFactory
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.XmlParser
import com.haima.sage.bigdata.etl.metrics.MeterReport

import scala.annotation.tailrec


/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/23 15:27.
  * 默认按照String 解析一行日志到，json
  */
class XMLLogLexer(override val parser: XmlParser, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer   {


  override val lexer: base.Lexer[String,RichMap] = XMLLogLexer()
}

object XMLLogLexer {
  def apply(parser: XmlParser = XmlParser()): base.Lexer[String, RichMap] = {
    new Lexer()
  }

  class Lexer() extends base.Lexer[String, RichMap] {

    /*val module = new JacksonXmlModule()


    module.setDefaultUseWrapper(false)
    val mapper: XmlMapper = new XmlMapper(module)*/

    lazy val factory: XmlFactory = {
      new XmlFactory()
    }
    lazy val inputFactory: XMLInputFactory = {
      System.setProperty("javax.xml.stream.XMLInputFactory", "com.ctc.wstx.stax.WstxInputFactory")
      val inputFactory: XMLInputFactory = XMLInputFactory.newFactory
      inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
      inputFactory
    }

    def add(data: java.util.Map[String, Any], key: String, value: Any): Unit = {
      val org = data.get(key)
      if (org != null)
        if (org.isInstanceOf[java.util.List[_]]) org.asInstanceOf[java.util.List[Any]].add(value)
        else {
          val obj = new java.util.ArrayList[Any]
          obj.add(org)
          data.put(key, obj)
          obj.add(value)
        }
      else data.put(key, value)
    }

    def tag(data: java.util.Map[String, Any])(implicit parser: FromXmlParser): java.util.Map[String, Any] = {
      val tag = if (parser.getCurrentName == "") {
        "value"
      } else {
        parser.getCurrentName
      }

      // print(tag + ":" + level + ">")
      val next = parser.nextToken()
      if (next == JsonToken.VALUE_STRING) {
        val title = parser.getValueAsString
        add(data, tag, title)
        data
      } else {
        val amap = new java.util.HashMap[String, Any]
        add(data, tag, amap)
        amap
      }
    }

    @tailrec
    private def tags(data: java.util.Map[String, Any])(implicit parser: FromXmlParser): Unit = {

      val tag = if (parser.getCurrentName == "") {
        "value"
      } else {
        parser.getCurrentName
      }
      val next = parser.nextToken()
      if (next == JsonToken.VALUE_STRING) {
        val value = parser.getValueAsString
        //print(s"[$title]")
        data.get(tag) match {
          case amap: java.util.HashMap[String@unchecked, Any@unchecked] =>
            amap.put(tag, value)
            parseChildren(amap)
          case _ =>
            add(data, tag, value)
        }
      } else {
        val amap = new java.util.HashMap[String, Any]
        add(data, tag, amap)
        parseChildren(amap)
      }


      parser.currentToken() match {
        case JsonToken.FIELD_NAME =>
          tags(data)
        case _ =>
      }
      //while (parser.currentToken() == JsonToken.FIELD_NAME) {}
    }

    @tailrec
    private def parseChildren(data: java.util.Map[String, Any])(implicit parser: FromXmlParser): Unit = {

      parser.nextToken()
      parser.currentToken() match {
        case JsonToken.FIELD_NAME =>
          tags(data)
          parseChildren(data)
        case obj =>

      }
      /*while (parser.currentToken() != null && (parser.currentToken() == JsonToken.FIELD_NAME)) {
        tags(data)
        parser.nextToken()
      }*/
    }

    def parse(from: String): RichMap = {



      val value: String = s"<root>${
        from.replaceAll("[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]", "")
          .replaceAll("(<([^/]*?)>[^<&>]*?)[<&>]([^<&>]*?</\\2>)", "$1$3").replaceAll("([^<])(/\\w+>)", "$1<$2")
      }</root>"
      try {
        /*
                import scala.collection.JavaConversions._

                val data = mapper.readValue(value, classOf[JMap[String, AnyRef]]).toMap
                //  flatMap(mapper.readValue(value, classOf[JMap[String, AnyRef]]))

                data*/
        _parse(value)
      }
      catch {
        case e: Exception =>
          // e.printStackTrace()
          logger.debug(s"xml parse error please check cause:${e.getMessage}")
          Map("error" -> s"xml parse error please check cause:${e.getMessage}")
      }

    }


    def _parse(from: String): RichMap = {
      import scala.collection.JavaConversions._
      val reader: XMLStreamReader = inputFactory.createXMLStreamReader(new ByteArrayInputStream(from.getBytes))
      implicit val parser: FromXmlParser = factory.createParser(reader)


      val data2: java.util.Map[String, Any] = new java.util.LinkedHashMap[String, Any]

      /* println("ccc" + parser.currentToken())
       while (parser.nextToken() != null) {
         if (parser.currentToken() == JsonToken.FIELD_NAME) {
           println(parser.currentToken() + ">" + parser.getCurrentName)
         } else {
           println(parser.currentToken())
         }

       }*/
      parser.nextToken()
      parseChildren(data2)
      data2.toMap
    }

  }

}
