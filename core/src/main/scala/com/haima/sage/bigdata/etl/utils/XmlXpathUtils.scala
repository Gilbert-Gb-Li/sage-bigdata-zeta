package com.haima.sage.bigdata.etl.utils

import java.io.ByteArrayInputStream

import com.haima.sage.bigdata.etl.common.model.RichMap
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.{XPath, XPathConstants, XPathExpression, XPathFactory}
import org.w3c.dom.{Document, Node, NodeList}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

object XmlXpathUtils {
  val cache = new mutable.WeakHashMap[String, XPathExpression]()

  /**
    *
    * @param index         字段名称
    * @param xpath         查找路径
    * @param trim          去除两边空白
    * @param nodeTypeFirst true单个节点，false多个节点
    */
  case class XpathConfig(index: String,
                         xpath: String,
                         trim: Boolean = true,
                         nodeTypeFirst: Boolean = true
                        )

  def getXPathExpression(path: String)(implicit xpath: XPath): XPathExpression = {
    val compile = cache.getOrElse(path, null)
    if (compile != null) {
      return compile
    }
    val newCompile = xpath.compile(path)
    cache.put(path, newCompile)
    newCompile
  }

  /**
    * xpath查找
    *
    * @param field       字段名
    * @param indices     1, index; 2, xpath
    * @param event       数据
    * @param emptyRemove 空值移除，默认true
    * @return
    */
  def xpath(field: String, indices: XpathConfig*)
           (implicit event: RichMap, emptyRemove: Boolean = true): RichMap = Try {
    if (indices == null || indices.isEmpty) {
      return event
    }
    val value: String = event.get(field) match {
      case Some(x: Any) =>
        x.toString
      case None => null
    }
    if (value == null) {
      return event
    }

    val dbf = DocumentBuilderFactory.newInstance()
    //实例化DocumentBuilderFactory对象
    val builder = dbf.newDocumentBuilder()
    val factory = XPathFactory.newInstance() //实例化XPathFactory对象，帮助创建XPath对象
    val xpath: XPath = factory.newXPath()
    val doc: Document = builder.parse(new ByteArrayInputStream(value.getBytes()))

    var dst = event
    for (config <- indices) {
      Try {
        val compile = getXPathExpression(config.xpath)(xpath)
        xmlNodeText(compile)(doc, config) match {
          case null => // 无值
            if (!emptyRemove) {
              dst = dst + (config.index -> null)
            }
          case x => // 有值
            dst = dst + (config.index -> x)
        }
      }
    }
    dst
  }.getOrElse(event)

  def xmlNodeText(compile: XPathExpression)(doc: Document, config: XpathConfig): String = {
    if (config.nodeTypeFirst) { // 单节点
      val node = compile.evaluate(doc, XPathConstants.NODE)
      if (node == null) {
        return null
      }
      val dst: String = node.asInstanceOf[Node].getTextContent

      trimValue(dst)(config)
    } else {
      val nodes = compile.evaluate(doc, XPathConstants.NODESET)
      if (nodes == null) {
        return null
      }
      val list = nodes.asInstanceOf[NodeList]
      findDomField(list)(config)
    }
  }

  def findDomField(nodes: NodeList)(config: XpathConfig): String = {
    val buf = new ListBuffer[String]
    for (i <- 0 until nodes.getLength) {
      val line = nodes.item(i).getTextContent
      buf += line
    }
    buf.mkString("\n")
  }

  def trimValue(value: String)(config: XpathConfig): String = {
    var dst = value
    //    if (valueType.)
    if (config.trim) {
      dst = dst.trim
    }
    dst match {
      case null | "" =>
        null
      case x =>
        x
    }
  }
}
