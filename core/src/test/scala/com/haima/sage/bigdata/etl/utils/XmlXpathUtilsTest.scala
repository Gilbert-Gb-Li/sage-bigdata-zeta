package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.utils.NumberUtils.textToLong
import com.haima.sage.bigdata.etl.utils.RegexFindUtils.{regex, regexBoth}
import com.haima.sage.bigdata.etl.utils.XmlXpathUtils.{XpathConfig, xpath}

import scala.io.Source

object XmlXpathUtilsTest extends Mapper {
  def read(str: String, encoding: String = "UTF-8"): String = {
    val in = getClass.getClassLoader.getResourceAsStream(str)
    val source = Source.fromInputStream(in, encoding)
    source.mkString
  }

  val event: com.haima.sage.bigdata.etl.common.model.RichMap = {
    import com.haima.sage.bigdata.etl.common.model.RichMap
    val xml = read("dump.xml")
    RichMap(Map[String, Any]("data" -> xml))
  }

  def main(args: Array[String]): Unit = {
    val res2 = xpath("data",
      XpathConfig("xx", "//node[@resource-id='com.uxin.usedcar:id/asn']"))(event)
    println(res2 - "data")

    implicit var dst: RichMap = event
    dst = xpath("data",
      XpathConfig("certificate_1",
        "//node[@resource-id='com.uxin.usedcar:id/aso']/node[@class='android.widget.TextView']/@text",
        nodeTypeFirst = false
      ),
      XpathConfig("certificate_2",
        "//node[@resource-id='com.uxin.usedcar:id/asu']/node[@class='android.widget.TextView']/@text",
        nodeTypeFirst = false
      ),
      XpathConfig("certificate_3",
        "//node[@resource-id='com.uxin.usedcar:id/jv']/node[@class='android.widget.TextView']/@text",
        nodeTypeFirst = false
      ),

      XpathConfig("down_payment", "//node[@resource-id='com.uxin.usedcar:id/ass']/@text"),

      XpathConfig("price", "//node[@resource-id='com.uxin.usedcar:id/asf']/@text"),
      XpathConfig("car_name", "//node[@resource-id='com.uxin.usedcar:id/asm']/@text"),

      XpathConfig("car_id", "//node[@resource-id='com.uxin.usedcar:id/aqu']/@text"),
      XpathConfig("registration_date", "//node[@resource-id='com.uxin.usedcar:id/aqw']/@text"),
      XpathConfig("car_color", "//node[@resource-id='com.uxin.usedcar:id/ar0']/@text"),
      XpathConfig("apparent_mileage", "//node[@resource-id='com.uxin.usedcar:id/ar4']/@text"),
      XpathConfig("engine_intake", "//node[@resource-id='com.uxin.usedcar:id/ar6']/@text"),
      XpathConfig("engine_exhaust", "//node[@resource-id='com.uxin.usedcar:id/ar7']/@text"),
      XpathConfig("lift_time", "//node[@resource-id='com.uxin.usedcar:id/oq']/@text"),
      XpathConfig("emission_standards", "//node[@resource-id='com.uxin.usedcar:id/ar9']/@text"),

      XpathConfig("check_date", "//node[@resource-id='com.uxin.usedcar:id/arg']/@text"),
      XpathConfig("check_people", "//node[@resource-id='com.uxin.usedcar:id/lk']/@text"),

      XpathConfig("last_maintenance_tmp", "//node[@resource-id='com.uxin.usedcar:id/bi8']/@text"),
      XpathConfig("fix_times", "//node[@text='维修次数']/../node[3]/@text"),
      XpathConfig("maintenance_times", "//node[@text='保养次数']/../node[3]/@text"),
      XpathConfig("accident_times", "//node[@text='事故次数']/../node[3]/@text"),

      XpathConfig("share_link", "//extra/@shared-link")
    )

    dst = regexBoth("car_name",
      "(?<brand>[^\\s]*)\\s(?<mode>.*)?(?<year>(20|19|21).*)款",
      "(手动|自动)(?<version>.*)")

    dst = regex("lift_time", "(?<lift_time_min>\\d+)-(?<lift_time_max>\\d+)")
    dst = regex("down_payment", "首付(?<down_payment>.*)万\\s月供(?<month_payment>.*)元")
    dst = regex("last_maintenance_tmp", "(?<last_maintenance_date>\\d{4}-\\d{1,2}-\\d{1,2})")
    dst

    val t1 = dst.getOrElse("apparent_mileage", "").toString.replace("公里", "")
    dst = dst + ("apparent_mileage" -> t1)

    dst = test2(dst)

    (dst - "data").toList.sortBy(_._1).foreach(println)
  }

  def test2(event: RichMap): RichMap = {
    implicit var dst: RichMap = event
    dst = textToLong("price", "apparent_mileage")

    dst
  }
}
