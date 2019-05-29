package com.haima.sage.bigdata.etl.streaming.flink.model

import java.text.{DateFormat, SimpleDateFormat}
import java.util.concurrent.TimeUnit

import com.haima.sage.bigdata.analyzer.streaming.StreamJoinAnalyzer
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.Test

class JoinableTest {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(CONF.getInt("flink.parallelism"))
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  @Test
  def join(): Unit = {


    val data = env.readTextFile("/Users/zhhuiyan/Downloads/timeseries-log.txt").map(d =>
      richMap(new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
        case ("@timestamp", value: String) =>
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          ("@timestamp", format.parse(value).getTime)
        case (key, value) =>
          (key, value)

      }))

    val data2 = env.readTextFile("/Users/zhhuiyan/Downloads/timeseries-log_forecast.txt").map(d =>
      richMap(new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
        case ("@timestamp", value: String) =>
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          ("@timestamp", format.parse(value).getTime)
        case (key, value) =>
          (key, value)

      }))
    val join = Join(JoinConfig("", "@timestamp", EventTime("@timestamp")),
      JoinConfig("", "@timestamp", EventTime("@timestamp")),window= TumblingWindow(12000, EventTime("@timestamp")))

    new StreamJoinAnalyzer(join).join(data, data2).writeAsText("./target/out.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)

  }

  @Test
  def split(): Unit = {
    val ps = "P   0054924171                      69511028                        402301099998MPS->400584020008NPS    NPS.143.001.01   2018-03-29 23:59:32 520   20180330000019395   8     MPSI0000"
      .split("\\s+")

    println(ps.mkString(","))
    val qs = "Q   15218862                        400584020008NPS->402701002999MPS    NPS.142.001.01   2018-03-29 00:00:19 440   20180330000019448   6     "
      .split("\\s+")
    println(qs.mkString(","))
  }

  @Test
  def join2(): Unit = {

    val format = new ThreadLocal[DateFormat]() with Serializable {
      protected override def initialValue(): DateFormat = {
        new SimpleDateFormat("yyyyMMddHHmmssSSS")
      }
    }


    val data = env.readTextFile("/Users/zhhuiyan/Desktop/tranlog.BK_01.EG01.2018-03-30.txt")
      .filter(_.startsWith("P")).map(d => {
      val d2 = d.split("\\s+")


      richMap(Map("tag" -> d2(0),
        "res_id" -> d2(1),
        "req_id" -> d2(2),
        "from_to" -> d2(3),
        "@timestamp" -> format.get().parse(d2(8))
      ))
    })
    val data2 = env.readTextFile("/Users/zhhuiyan/Desktop/tranlog.BK_01.EG01.2018-03-30.txt")
      .filter(_.startsWith("Q")).map(d => {
      val d2 = d.split("\\s+")


      richMap(Map("tag2" -> d2(0),
        "req_id2" -> d2(1),
        "from_to2" -> d2(2),
        "@timestamp2" -> format.get().parse(d2(7))

      ))
    })
    val join = Join(JoinConfig("", "req_id", EventTime("@timestamp",10000L) ),
      JoinConfig("", "req_id2", EventTime("@timestamp2")),window=TumblingWindow(12000, EventTime("@timestamp")))

    new StreamJoinAnalyzer(join).join(data, data2).writeAsText("./target/out2.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)

  }

}
