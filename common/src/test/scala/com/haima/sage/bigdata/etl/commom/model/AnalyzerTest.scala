package com.haima.sage.bigdata.etl.commom.model

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

import scala.util.Try

class AnalyzerTest extends Mapper {

  @Test
  def analyzer(): Unit = {
    val vin: Window = TumblingWindow(1000, EventTime("timestamp"))
    println(mapper.writeValueAsString(vin))

    println(mapper.readValue[Window](mapper.writeValueAsString(vin)))
  }

  @Test
  def timeseries(): Unit = {
    val source = TimeSeriesAnalyzer("b", "eventTime",HoltWinters(6))


    assert(Try(mapper.writeValueAsString(source)).isSuccess)
    val json = mapper.writeValueAsString(source)
    println(json)
    val obj = Try(mapper.readValue[Option[TimeSeriesAnalyzer]](json))
    assert(obj.isSuccess, obj.failed.get.fillInStackTrace())
    val data =
      """{"field":"cpu",
        |"timeField":"@timestamp",
        |"window":3600,
        |"slide":600,
        |"model":{
        |   "period":600,
        |   "name":"holtwinters",
        |   "modelType":"ADDITIVE"
        |},
        |"forecast":100,
        |"samplingDensity":1.0,
        |"name":"timeseries"}""".stripMargin
    val obj2 = Try(mapper.readValue[Option[TimeSeriesAnalyzer]](data))
    assert(obj2.isSuccess)
    val holwinters = obj2.get.get.timeSeriesModel.asInstanceOf[HoltWinters]
    assert(holwinters.period == 600)
    assert(holwinters.modelType == HoltWintersType.ADDITIVE)
  }
}
