package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.utils.XmlToTextUtils.xmlToText
import com.haima.sage.bigdata.etl.utils.XmlXpathUtilsTest._

object XmlToTextUtilsTest {
  val event: com.haima.sage.bigdata.etl.common.model.RichMap = {
    import com.haima.sage.bigdata.etl.common.model.RichMap
    val xml = read("dump.xml")
    RichMap(Map[String, Any]("data" -> xml, "price" -> "2.1万"))
  }

  def main(args: Array[String]): Unit = {
    val res = xmlToText(event)
    println(res.getOrElse("raw_text", null))
  }

}
