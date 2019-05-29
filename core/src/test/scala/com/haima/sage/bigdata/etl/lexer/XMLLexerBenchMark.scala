package com.haima.sage.bigdata.etl.lexer

import java.io.ByteArrayInputStream
import javax.xml.stream.{XMLInputFactory, XMLStreamReader}

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.fasterxml.jackson.dataformat.xml.{JacksonXmlModule, XmlFactory, XmlMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.junit.Test

/**
  * Created by Dell on 2017-08-31.
  */
class XMLLexerBenchMark {

  import scala.xml._

  implicit object NodeFormat {
    def write(node: Node): Any = {

      val childs = node.child.filter(!_.isInstanceOf[Text])
      val labels = childs.map(_.label).toSet
      if (node.child.count(_.isInstanceOf[Text]) == 1)
        node.text
      else {
        Map(node.label -> {
          if (labels.size == 1) {
            Map(labels.head -> node.child.collect {
              case e: Elem => write(e) match {
                case data: Map[String@unchecked, Any@unchecked] if data.size == 1 =>
                  data.values.head
                case obj =>
                  obj
              }
            })
          } else {
            node.child.collect {
              case e: Elem => e.label -> write(e)
            }.toMap
          }
        })


      }

    }

  }

  val factory = {
    new XmlFactory()
  }
  val inputFactory = {
    System.setProperty("javax.xml.stream.XMLInputFactory", "com.ctc.wstx.stax.WstxInputFactory")
    val inputFactory: XMLInputFactory = XMLInputFactory.newFactory
    inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
    inputFactory
  }

  def parser(from: String): Map[String, Any] = {
    import scala.collection.JavaConversions._
    val reader: XMLStreamReader = inputFactory.createXMLStreamReader(new ByteArrayInputStream(from.getBytes))
    implicit val parser: FromXmlParser = factory.createParser(reader)

    def add(data: java.util.Map[String, Any], key: String, value: Any): Unit = {
      val org = data.get(key)
      if (org != null) if (org.isInstanceOf[java.util.List[_]]) org.asInstanceOf[java.util.List[Any]].add(value)
      else {
        val obj = new java.util.ArrayList[Any]
        obj.add(org)
        data.put(key, obj)
        obj.add(value)
      }
      else data.put(key, value)
    }

    def addTag(data: java.util.Map[String, Any])(implicit parser: FromXmlParser): Unit = {


      // print(tag + ":" + level + ">")
      parser.nextToken()
      if (parser.currentToken() != null && (parser.currentToken() == JsonToken.FIELD_NAME)) {

        val tag = if (parser.getCurrentName == "") {
          "value"
        } else {
          parser.getCurrentName
        }
        val next = parser.nextToken()
        if (next == JsonToken.VALUE_STRING) {
          val title = parser.getValueAsString
          add(data, tag, title)
          addTag(data)
        } else {
          val rt = new java.util.HashMap[String, Any]
          add(data, tag, rt)
          addTag(rt)
        }
      } else {
        parser.nextToken()
        while (parser.currentToken() != null && (parser.currentToken() == JsonToken.END_OBJECT)){
          parser.nextToken()
        }
        if (parser.currentToken() != null && (parser.currentToken() == JsonToken.FIELD_NAME)) {
          addTag(data)
        }
      }


    }

    def tags(data: java.util.Map[String, Any]) = {


      val rt = new java.util.HashMap[String, Any]
      while (parser.currentToken() == JsonToken.FIELD_NAME) {
        val tag = if (parser.getCurrentName == "") {
          "value"
        } else {
          parser.getCurrentName
        }

        // print(tag + ":" + level + ">")
        val next = parser.nextToken()
        if (next == JsonToken.VALUE_STRING) {
          val title = parser.getValueAsString
          //print(s"[$title]")
          if (!rt.isEmpty)
            rt.put(tag, title)
          if (rt.isEmpty) {
            add(data, tag, title)
          } else {
            add(data, tag, rt)
            //+1
            parseAndPrintChildren(rt)
          }


          /**/
        } else {
          add(data, tag, rt)
          //+1
          parseAndPrintChildren(rt)
        }
      }

      rt
    }

    def parseAndPrintChildren(data: java.util.Map[String, Any]): Unit = {

      parser.nextToken()
      while (parser.currentToken() != null && (parser.currentToken() == JsonToken.FIELD_NAME)) {
        tags(data)
        parser.nextToken()
      }
      //println("end " + parser.currentToken())
    }

    val data2: java.util.Map[String, Any] = new java.util.LinkedHashMap[String, Any]
    parser.nextToken()

  //  parseAndPrintChildren(data2)
    addTag(data2)
    data2.toMap
  }


  def parseN(from: String): Map[String, Any] = {


    import javax.xml.stream._

    import scala.collection.JavaConversions._
    val data: String = from.replaceAll("[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]", "")
      .replaceAll("(<([^/]*?)>[^<&>]*?)[<&>]([^<&>]*?</\\2>)", "$1$3").replaceAll("([^<])(/\\w+>)", "$1<$2")
    val reader: XMLStreamReader = inputFactory.createXMLStreamReader(new ByteArrayInputStream(data.getBytes))
    val rt: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]
    val stack: java.util.Stack[java.util.Map[String, AnyRef]] = new java.util.Stack[java.util.Map[String, AnyRef]]
    var name: String = ""
    try {
      while (reader.hasNext) {
        reader.next
        if (reader.isStartElement) {
          val temp = new java.util.HashMap[String, AnyRef]
          name = reader.getLocalName.toLowerCase()
          if (stack.size == 0) {
            rt.put(name, temp)
          }
          else {
            stack.peek.put(name, temp)
          }
          stack.push(temp)
          (0 until reader.getAttributeCount).foreach(i => {
            stack.peek.put(reader.getAttributeLocalName(i).toLowerCase(), reader.getAttributeLocalName(i))
          })

        }

        if (reader.isEndElement) {
          if (stack.peek.size == 0) {
            stack.pop
            stack.peek.put(reader.getLocalName.toLowerCase(), null)
          }
          else if (!stack.peek.containsKey(reader.getLocalName.toLowerCase())) {
            stack.pop
          }
        }
        if (reader.isCharacters) {
          if (!reader.isWhiteSpace) {
            if (stack.peek.size == 0) {
              stack.pop
              stack.peek.put(name, reader.getElementText)
            }
            else {
              stack.peek.put("value", reader.getElementText)
            }
          }
        }

      }
      stack.empty()
      rt.toMap
    } catch {
      case e: Exception =>
        Map("error" -> s"xml parse error please check cause:${e.getMessage}")
    } finally {
      reader.close()
    }


  }

  @Test
  def read(): Unit = {

    val json =
      """<root><recordInfo>
        |<fieldInfo1>1</fieldInfo1>
        |<fieldInfo>
        |<fieldChName v2="sss">网络IP</fieldChName>
        |<fieldEnName>c60000010</fieldEnName>
        |<fieldContent><![CDATA[10.1.222.140]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName v2="dd">安全域</fieldChName>
        |<fieldEnName>c60000008</fieldEnName>
        |<fieldContent><![CDATA[IT管理区]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName>类型</fieldChName>
        |<fieldEnName>type</fieldEnName>
        |<fieldContent><![CDATA[网络]]></fieldContent>
        |</fieldInfo>
        |</recordInfo><recordInfo>
        |<fieldInfo1>2</fieldInfo1>
        |<fieldInfo>
        |<fieldChName v2="sss">网络IP</fieldChName>
        |<fieldEnName>c60000010</fieldEnName>
        |<fieldContent><![CDATA[10.1.222.140]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName v2="dd">安全域</fieldChName>
        |<fieldEnName>c60000008</fieldEnName>
        |<fieldContent><![CDATA[IT管理区]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName>类型</fieldChName>
        |<fieldEnName>type</fieldEnName>
        |<fieldContent><![CDATA[网络]]></fieldContent>
        |</fieldInfo>
        |</recordInfo><recordInfo>
        |<fieldInfo1>3</fieldInfo1>
        |<fieldInfo>
        |<fieldChName v2="sss">网络IP</fieldChName>
        |<fieldEnName>c60000010</fieldEnName>
        |<fieldContent><![CDATA[10.1.222.140]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName v2="dd">安全域</fieldChName>
        |<fieldEnName>c60000008</fieldEnName>
        |<fieldContent><![CDATA[IT管理区]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName>类型</fieldChName>
        |<fieldEnName>type</fieldEnName>
        |<fieldContent><![CDATA[网络]]></fieldContent>
        |</fieldInfo>
        |</recordInfo><recordInfo>
        |<fieldInfo1>4</fieldInfo1>
        |<fieldInfo>
        |<fieldChName v2="sss">网络IP</fieldChName>
        |<fieldEnName>c60000010</fieldEnName>
        |<fieldContent><![CDATA[10.1.222.140]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName v2="dd">安全域</fieldChName>
        |<fieldEnName>c60000008</fieldEnName>
        |<fieldContent><![CDATA[IT管理区]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName>类型</fieldChName>
        |<fieldEnName>type</fieldEnName>
        |<fieldContent><![CDATA[网络]]></fieldContent>
        |</fieldInfo>
        |</recordInfo></root>""".stripMargin

    val json2 = """<a><b>1</b><b>2</b><b>2</b></a>""".stripMargin

    val module = new JacksonXmlModule()
    module.setDefaultUseWrapper(true)


    val mapper: XmlMapper = new XmlMapper with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule).setSerializationInclusion(Include.NON_ABSENT)


    //  module.addDeserializer(classOf[util.Map[String, AnyRef]],new   ListOfMapEntryDeserializer())
    val engine = "fasterxml2"
    println(engine)
    val start2 = System.currentTimeMillis()
    engine match {
      case "fasterxml" =>

        //println(mapper.readValue(json, new TypeReference[util.Map[String, AnyRef]]() {}))
        (0 to 1).foreach(i => {
          val data = mapper.readValue(json, classOf[Map[String, Any]])
          println(data)

        })
      /* val data = mapper.readTree(json)

       println(mapper.writeValueAsString(data))
       data.elements().foreach(println(_))

       def printData(data: JsonNode): Unit = {


         data.elements().foreach {
           node: JsonNode =>
             // println(entry.getKey)
             printData(node)

         }
       }

       printData(data)*/
      case "stax" =>
        parseN(json)
        (0 to 10000).foreach(i => {
          parseN(json.replaceAll("type", "type" + i).replaceAll("类型", "类型" + i).replaceAll("网络", "网络" + i))

        })
      case "fasterxml2" =>

        (0 to 1).foreach(i => {
          println(parser(json.replaceAll("type", "type" + i).replaceAll("类型", "类型" + i).replaceAll("网络", "网络" + i)))

        })

      case _ =>

        (0 to 10000).foreach(i => {
          val dd = XML.loadString(json)
          NodeFormat.write(dd)

        })

    }
    println(s"$engine parse 10000 items take ${System.currentTimeMillis() - start2} ms")


  }


}
