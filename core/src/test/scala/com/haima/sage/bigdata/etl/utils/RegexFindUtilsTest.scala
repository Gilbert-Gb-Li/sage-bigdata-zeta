package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.utils.NumberUtils.textToLong
import com.haima.sage.bigdata.etl.utils.RegexFindUtils.{regex, regexBoth}

object RegexFindUtilsTest {
  val events: Seq[RichMap] = {
    val list = Seq("长安欧尚 长安之星 2008款 1.0 手动",
      "理念 理念S12014款 1.3 手动 舒适版",
      "大众 高尔夫GTI 2016款 2.0T 自动",
      "大众 途观2016款 1.8T 自动 300TSI风尚视野版四驱",
      "现代 瑞纳三厢2010款 1.4 手动 GS舒适型",
      "理念 理念S12014款 1.3 手动 舒适版",
      "大众 Cross POLO 2012款 1.6 自动",
      "东风风神 风神A602016款 1.5 手动 豪华型",
      "五菱汽车 宏光2015款 1.2 手动 S超值版5-8座",
      "长安 悦翔V72015款 1.6 手动 乐享型 国IV",
      "长安 逸动2014款 1.6 手动 豪华型")

    list.map(item => {
      val date = if (item.contains("2014")) {
        "最近一次维保时间： 未知"
      } else {
        "最近一次维保时间： 2018-09-21"
      }
      RichMap(Map[String, Any]("car_name" -> item,
        "price" -> "2.1万",
        "apparent_mileage" -> "0.3w",
        "last_maintenance_tmp" -> date
      ))
    })
  }

  def main(args: Array[String]): Unit = {
    events.foreach(event => {
      implicit var dst: RichMap = event
      dst = textToLong("price", "apparent_mileage")
      dst = regexBoth("car_name", "(?<brand>[^\\s]*)\\s(?<mode>.*)?(?<year>(20|19|21).*)款",
        "(手动|自动)(?<version>.*)")

      dst = regex("last_maintenance_tmp", "(?<last_maintenance_date>\\d{4}-\\d{1,2}-\\d{1,2})")

      dst
      println(dst - "car_name")
    })
  }
}
