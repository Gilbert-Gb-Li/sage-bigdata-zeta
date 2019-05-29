package com.haima.sage.bigdata.etl.performance.test

import com.haima.sage.bigdata.etl.common.model.RichMap

class YYDanmuParserTest {

  def danmuParse(event: RichMap): Unit ={

    event.getOrElse("gift_id", null) match {
      case null =>
        event + ("gift_num" -> 0)
      case _ =>
        val content = event.getOrElse("content", "").toString
        val r = "送.{0,}\\d个".r
        val r1 = "\\d{0,}个$".r
        r1.findFirstIn(
          r.findFirstIn(content)
            .mkString) match {
          case Some(x) =>
            event + ("gift_num" ->x.replace("个", "").toInt)
          case None => event + ("gift_num" -> 0)
        }
    }
  }

}

object YYDanmuParserTest{

  def main(args: Array[String]): Unit = {
    val yy = new YYDanmuParserTest()
    val event: RichMap = RichMap(Map("gift_id"->"abc",
      "content" -> "[role]无情 送给6号嘉宾1个爱心{100012}"
    ))
    yy.danmuParse(event)
    println("gift_num: ", event.getOrElse("gift_num", -1))
  }


}
