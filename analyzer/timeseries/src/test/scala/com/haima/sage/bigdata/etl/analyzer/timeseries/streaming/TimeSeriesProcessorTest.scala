package com.haima.sage.bigdata.analyzer.timeseries.streaming

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.analyzer.streaming.filter.StreamAnalyzerProcessor
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.junit.Test

class TimeSeriesProcessorTest extends Serializable {


  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(CONF.getInt("flink.parallelism"))
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  Constants.init("sage-analyzer-timeseries.conf")


  //val time = new Date().getTime

  //  val datas: Array[Double] = {
  //    Array(1, 2, 3, 4, 3, 2,
  //      1, 2, 3, 4, 3, 2,
  //      1, 2, 3, 4, 3, 2,
  //      1, 2, 3, 4, 3, 2,
  //      1, 2, 3, 4, 3, 2)
  //  }
  val datas: Array[Double] = {
    Array(
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
      508.0, 461.0, 390.0, 432.0)
  }
  val time = new Date()
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val cal: Calendar = Calendar.getInstance()
  cal.setTime(time)


  protected val data: DataStream[RichMap] = env.fromElements(
    datas.zipWithIndex.map {
      case (data, index) if index % 2 == 0 =>
        richMap(Map("a" -> "x", "b" -> data, "@timestamp" -> {
          cal.add(Calendar.SECOND, 1)
          cal.getTime
        }))
      case (data, index) =>
        richMap(Map("a" -> "y", "b" -> data, "@timestamp" -> {
          cal.add(Calendar.SECOND, 1)
          cal.getTime
        }))
    }: _*
  )

  @Test
  def testKeybythenWindows(): Unit = {
    val stream = env.fromElements(List[Map[String, Any]](Map("a" -> ("a" -> 1)),
      Map("b" -> ("b" -> 2)),
      Map("c" -> ("c" -> 3)),
      Map("d" -> ("d" -> 4))))

    val stream2 = stream.flatMap(_.toList).map { d => {
      val head = d.head

      d.head match {
        case (key: String, (k: String, v: Double)) =>
          (key, k, v)
      }
    }
    }
    stream2.print()
    data.keyBy(m => m.getOrElse("a", "")).map(d => d.get("a")).print()
    //.writeAsText("./target/out.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
  }


  @Test
  def windowSlideForecast2(): Unit = {
    val data: DataStream[RichMap] = env.readTextFile("C:\\logs_rds.txt").map(d =>
      new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
        case ("@timestamp", value: String) =>
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          ("@timestamp", format.parse(value).getTime)
        case (key, value) =>
          (key, value)
      })

    val result = new StreamTimeSeriesAnalyzer(TimeSeriesAnalyzer("transaction_number", "@timestamp",

      HoltWinters(7 * 24 * 3600, 0, 0, 0, HoltWintersType.ADDITIVE),
      forecast = 24 * 3600,
      keys = "endpoint_id",
      backend = "tags,endpoint_id",
      window = Some(SlidingWindow(24 * 3600, 30 * 24 * 3600, EventTime("@timestamp", 7 * 24 * 3600)))


    )).action(data)
    result.writeAsText("./target/out.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)
    //    val rt = Source.fromFile("./target/aa.txt").getLines()
    //    EasyPlot.ezplot(datas ++ rt.map(_.split(",")(0).toDouble).toArray[Double])
    //    TimeUnit.SECONDS.sleep(600)
  }

  @Test
  def windowSlideForecast(): Unit = {

    val conf = ReAnalyzer(Some(TimeSeriesAnalyzer("b", "@timestamp",
      ARIMA(coefficients = Array()), forecast = 10,
      window = Some(SlidingWindow(60, 60, EventTime("@timestamp", 7 * 24 * 3600))))), Some("additive"))
    val processor = StreamAnalyzerProcessor(conf)
    assert(processor.engine() == AnalyzerModel.STREAMING)
    // val result = processor.process(data).head

    val result = new StreamTimeSeriesAnalyzer(TimeSeriesAnalyzer("b", "@timestamp",
      ARIMA(coefficients = Array()), forecast = 10,
        window = Some(SlidingWindow(60, 60, EventTime("@timestamp", 7 * 24 * 3600))))
    ).action(data)
    result.writeAsText("./target/out.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)

    //    val rt = Source.fromFile("./target/aa.txt").getLines()
    //    EasyPlot.ezplot(datas ++ rt.map(_.split(",")(0).toDouble).toArray[Double])
    //    TimeUnit.SECONDS.sleep(600)


  }
}
