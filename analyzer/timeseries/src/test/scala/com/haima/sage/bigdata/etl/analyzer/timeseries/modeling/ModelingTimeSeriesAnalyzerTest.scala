package com.haima.sage.bigdata.analyzer.timeseries.modeling


import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date, Locale}

import com.haima.sage.bigdata.analyzer.timeseries.EasyPlot
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.ReAnalyzer
import com.haima.sage.bigdata.analyzer.modeling.filter.ModelingAnalyzerProcessor
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.junit.Test

/**
  * Created by CaoYong on 2017/11/6.
  */
class ModelingTimeSeriesAnalyzerTest {
  private val env = ExecutionEnvironment.getExecutionEnvironment
  Constants.init("sage-analyzer-timeseries.conf")

  val airPessenger = Array(
    112.0, 118.0, 132.0, 129.0, 121.0, 135.0, 148.0, 148.0, 136.0, 119.0, 104.0, 118.0, 115.0, 126.0,
    141.0, 135.0, 125.0, 149.0, 170.0, 170.0, 158.0, 133.0, 114.0, 140.0, 145.0, 150.0, 178.0, 163.0,
    172.0, 178.0, 199.0, 199.0, 184.0, 162.0, 146.0, 166.0, 171.0, 180.0, 193.0, 181.0, 183.0, 218.0,
    230.0, 242.0, 209.0, 191.0, 172.0, 194.0, 196.0, 196.0, 236.0, 235.0, 229.0, 243.0, 264.0, 272.0,
    237.0, 211.0, 180.0, 201.0, 204.0, 188.0, 235.0, 227.0, 234.0, 264.0, 302.0, 293.0, 259.0, 229.0,
    203.0, 229.0, 242.0, 233.0, 267.0, 269.0, 270.0, 315.0, 364.0, 347.0, 312.0, 274.0, 237.0, 278.0,
    284.0, 277.0, 317.0, 313.0, 318.0, 374.0, 413.0, 405.0, 355.0, 306.0, 271.0, 306.0, 315.0, 301.0,
    356.0, 348.0, 355.0, 422.0, 465.0, 467.0, 404.0, 347.0, 305.0, 336.0, 340.0, 318.0, 362.0, 348.0,
    363.0, 435.0, 491.0, 505.0, 404.0, 359.0, 310.0, 337.0, 360.0, 342.0, 406.0, 396.0, 420.0, 472.0,
    548.0, 559.0, 463.0, 407.0, 362.0, 405.0, 417.0, 391.0, 419.0, 461.0, 472.0, 535.0, 622.0, 606.0,
    508.0, 461.0, 390.0, 432.0
  )

  val time = new Date()
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val cal: Calendar = Calendar.getInstance()
  cal.setTime(time)
  protected val data: DataSet[RichMap] = env.fromElements(
    airPessenger.zipWithIndex.map {
      case (data, index) =>
        richMap(Map("a" -> "x", "b" -> data, "@timestamp" -> {
          cal.add(Calendar.SECOND, 1)
          cal.getTime
        }))
    }: _*
  )

  @Test
  def timeSeriesHoltWintersForeast(): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:sss")
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA)
    var d = df.parse("2017-07-30T08:01:40+08:00")
    val conf = TimeSeriesAnalyzer("b", "@timestamp", HoltWinters(12), forecast = 10)
    val processor = new ModelingTimeSeriesAnalyzer(conf, AnalyzerType.ANALYZER)
    val result = processor.action(data).map(data => {
      data.get("b").map(_.asInstanceOf[Double]).getOrElse(0d)
    }).collect().toArray
    println(result.length)
    //assert(result.length == 1)
    EasyPlot.ezplot(airPessenger)
    EasyPlot.acfPlot(airPessenger, 20)
    EasyPlot.pacfPlot(airPessenger, 20)
    EasyPlot.ezplot(airPessenger ++ result)
    TimeUnit.SECONDS.sleep(600)
  }

  @Test
  def timeSeriesARIMAForeast(): Unit = {
    val conf = ReAnalyzer(Some(TimeSeriesAnalyzer("b", "@timestamp",
      ARIMA(coefficients = Array()), forecast = 10)), Some("additive"))
    val processor = ModelingAnalyzerProcessor(conf)
    val result = processor.process(data).head.map(data => {
      (data.get("b").map(_.asInstanceOf[Double]).getOrElse(0d))
    }).collect().toArray
    assert(result.length == 10 + airPessenger.length)
  }

  @Test
  def timeSeriesARIMAForecast2(): Unit = {
    val data: DataSet[RichMap] = env.readTextFile("C:\\Users\\dell\\Desktop\\timeseries-log.json").map(d =>
      new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
        case ("@timestamp", value: String) =>
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          ("@timestamp", format.parse(value).getTime)
        case (key, value) =>
          (key, value)
      })

    val result = new ModelingTimeSeriesAnalyzer(TimeSeriesAnalyzer("usage_user,usage_system", "@timestamp",
      ARIMA(2, 0, 2, coefficients = Array(0.1932349123578884)),
      forecast = 10 * 60,
      keys = "endpoint_id",
      backend = "endpoint_id"
    ), AnalyzerType.ANALYZER).action(data).collect().foreach(println)
    //    val rt = Source.fromFile("./target/aa.txt").getLines()
    //    EasyPlot.ezplot(datas ++ rt.map(_.split(",")(0).toDouble).toArray[Double])
    //    TimeUnit.SECONDS.sleep(600)
  }

  @Test
  def timeSeriesHoltWintersForecast2(): Unit = {
    val data: DataSet[RichMap] = env.readTextFile("C:\\Users\\dell\\Desktop\\timeseries-log.json").map(d =>
      new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
        case ("@timestamp", value: String) =>
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          ("@timestamp", format.parse(value).getTime)
        case (key, value) =>
          (key, value)
      })

    val result = new ModelingTimeSeriesAnalyzer(TimeSeriesAnalyzer("usage_user,usage_system", "@timestamp",
      HoltWinters(300),
      forecast = 10 * 60,
      keys = "endpoint_id",
      backend = "name,category_name,display_name,endpoint,category_code,endpoint_id,endpoint_name"
    ), AnalyzerType.ANALYZER).action(data).collect().foreach(println)
  }

  @Test
  def testmodeling(): Unit = {
    val data: DataSet[RichMap] = env.readTextFile("C:\\timeseries-log.json").map(d =>
      new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
        case ("@timestamp", value: String) =>
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          ("@timestamp", format.parse(value).getTime)
        case (key, value) =>
          (key, value)
      })

    val result = new ModelingTimeSeriesAnalyzer(TimeSeriesAnalyzer("usage_user,usage_system", "@timestamp",
      ARIMA(2, 0, 2, coefficients = Array(0.1932349123578884)),
      forecast = 10 * 60,
      keys = "endpoint_name",
      backend = "endpoint_id",
      window = Some(SlidingWindow(10 * 60, 60 * 60, EventTime("@timestamp", maxOutOfOrderness = 60 * 60 * 1000L)))), AnalyzerType.MODEL).modelling(data).collect().foreach(println)
  }

  @Test
  def Test(): Unit = {
    var ma = Map[String, Any]()
    ma += ("a" -> 1)
    ma += ("b" -> 2)
    ma += ("c" -> 3)
    val mapper = new Mapper() {}.mapper
    var result = mapper.writeValueAsString(ma)
    var d = Map[String, Any]("groups" -> result)
    val groups = mapper.readValue[Map[String, Any]](d("groups").toString)
    val keys = groups.keys
    val values = groups.values.mkString(",")
    println(values)
  }
  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
  @Test
  def tt(): Unit ={
    val a ="1516804200000"
    println(tranTimeToString(a))
  }
}
