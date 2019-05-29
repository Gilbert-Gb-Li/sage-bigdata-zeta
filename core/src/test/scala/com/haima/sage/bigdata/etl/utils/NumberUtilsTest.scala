package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.utils.NumberUtils._
import com.haima.sage.bigdata.etl.utils.XmlXpathUtilsTest._

object NumberUtilsTest {
  val event: com.haima.sage.bigdata.etl.common.model.RichMap = {
    import com.haima.sage.bigdata.etl.common.model.RichMap
    val xml = read("dump.xml")
    RichMap(Map[String, Any]("data" -> xml, "price" -> "2.1万"))
  }

  def main(args: Array[String]): Unit = {
    val res1 = textToLong("price", "apparent_mileage")(event)
    println(res1)
    val res2 = textToDouble("price", "apparent_mileage")(event)
    println(res2)
  }
}
