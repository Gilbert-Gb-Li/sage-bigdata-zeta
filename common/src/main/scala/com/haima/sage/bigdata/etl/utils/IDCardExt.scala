/**
  * @date 2015年12月14日 上午10:31:54
  */
package com.haima.sage.bigdata.etl.utils

import java.io._
import java.util.stream.Collectors


/**
  * 2015年12月14日 上午10:31:54
  */
object IDCardExt {
  /**
    * ID	BM	DQ
    * 1	110000	北京市
    *
    **/
  private def path(fileName: String): String = {
    var filePath: String = fileName
    if (!fileName.startsWith("/")) {
      val url = classOf[IPExt].getClassLoader.getResource(fileName)
      filePath = url.getPath
    }
    if (filePath.startsWith("file")) filePath = filePath.substring(5)
    filePath = filePath.replace("/", File.separator)
    filePath
  }

  val in = new BufferedReader(new FileReader(path("idcard.csv")))

  import scala.collection.JavaConversions._

  val records: Map[Int, Map[String, Any]] = in.lines.collect(Collectors.toList()).map {
    line =>
      val values = line.split(",")
      val number = values(1).toInt
      (number, Map("regionName" -> values(0), "regionCode" -> number, "regionSimpleName" -> values(2),
        "regionSimpleCode" -> values(3).toInt, "regionFullName" -> values(4)))

  }.toMap


  in.close()


  def load(path: String) {

  }

  def find(number: String): Map[String, Any] = {
    if (number.charAt(0) <= '0' || number.charAt(0) > '9' || (number.length() != 18 && number.length != 15)) return Map("regionUnknown" -> "非中华人民共和国身份证!")

    records.getOrElse(number.substring(0, 6).toInt, Map("regionError" -> "所配置的地址不在系统中,或者是已经弃用的号码,请核验!"))
  }

}