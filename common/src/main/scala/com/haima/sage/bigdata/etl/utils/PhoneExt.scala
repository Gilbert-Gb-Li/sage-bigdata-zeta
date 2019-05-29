/**
  * @date 2015年12月14日 上午10:31:54
  */
package com.haima.sage.bigdata.etl.utils

import java.io._
import java.util.stream.Collectors


/**
  * 2015年12月14日 上午10:31:54
  */
object PhoneExt {
  /**
    * ID	BM	DQ
    * 1	110000	北京市
    *
    **/


  val in = new BufferedReader(new FileReader("idcard.csv"))

  import scala.collection.JavaConversions._

  val records: Map[Int, Map[String, Any]] = in.lines.collect(Collectors.toList()).map {
    line =>
      val values = line.split(",")
      val number = values(1).toInt
      (number, Map( "number" -> number, "regionName" -> values(2),"provider" -> values(3),
        "regionCode" -> values(3).toInt, "orgCode" -> values(4)))

  }.toMap


  in.close()


  def load(path: String) {

  }

  def find(number: String): Map[String, Any] = {
    if (number.length() < 6) return null

    records(number.substring(0, 6).toInt)
  }
}