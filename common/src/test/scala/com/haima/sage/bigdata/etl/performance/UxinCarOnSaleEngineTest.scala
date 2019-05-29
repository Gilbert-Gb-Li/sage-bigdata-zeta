package com.haima.sage.bigdata.etl.performance

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.performance.utils.{ScriptEngineUtils, TestUtils}

object UxinCarOnSaleEngineTest {
  val event: RichMap = {
    val xml = TestUtils.readAll("car-info.xml")
    RichMap(Map[String, Any]("data" -> xml))
  }

  def main(args: Array[String]): Unit = {
    engineTest(1000)
  }


  def engineTest(times: Int = 1): Unit = {
    val base = "C:\\Users\\Administrator\\idea\\sage-bigdata-zeta\\common\\src\\test\\scala\\com\\haima\\sage\\bigdata\\etl\\performance\\service"
    val files = Seq(
      s"$base\\BaseConditionService.scala",
      s"$base\\NumberService.scala",
      s"$base\\RegexService.scala",
      s"$base\\ReplaceService.scala",
      s"$base\\TextMatchService.scala",
      s"$base\\XmlToTextService.scala",
      s"$base\\FiledTypeService.scala",
      s"$base\\MergeService.scala",
      s"$base\\GuajiCarPriceInfoUtils.scala"
    )

    val invoke =
      """
        |GuajiCarPriceInfoUtils.process(event)
      """.stripMargin
    val script = ScriptEngineUtils.genScript(files, invoke)

    ScriptEngineUtils.process(event, script, times)

  }
}
