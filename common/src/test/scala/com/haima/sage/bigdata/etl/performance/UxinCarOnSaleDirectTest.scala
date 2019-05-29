package com.haima.sage.bigdata.etl.performance

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.performance.service._
import com.haima.sage.bigdata.etl.performance.utils.TestUtils


object UxinCarOnSaleDirectTest {
  val event: RichMap = {
    val xml = TestUtils.readAll("car-info.xml")
    RichMap(Map[String, Any]( "data" -> xml,
    "schema" ->
      "dump://com.ganji.android.haoche_c/com.ganji.android.haoche_c.ui.detail.CarDetailsActivity;task_type=DUMP;intent:#Intent;launchFlags=0x10000000;component=com.ganji.android.haoche_c/.ui.detail.CarDetailsActivity;B.is_from_push=false;S.puid=3008336208;end/click:-1_-1/scroll:1_400=10/"
    ))
  }

  def main(args: Array[String]): Unit = {
      val begin = System.currentTimeMillis()
      val res = GuajiCarInfoUtils.process(event)
      (res -  "c@raw" - "data" - "schema").toList.foreach(println)
      val end = System.currentTimeMillis()
      val inter = end - begin
      println(s"it took: $inter")
  }


}
