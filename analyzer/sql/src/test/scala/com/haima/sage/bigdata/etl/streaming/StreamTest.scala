package com.haima.sage.bigdata.etl.streaming

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.haima.sage.bigdata.etl.common.Constants.CONF
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Expression, ProctimeAttribute, RowtimeAttribute, UnresolvedFieldReference}
import org.apache.flink.types.Row
import org.junit.Test

import scala.util.Random





/**
  * Created by zhhuiyan on 2017/5/18.
  */
class StreamTest extends Serializable {
  @Test
  def transform(): Unit ={
    val time:Option[Long] = Some(1)
    val t:Long = time.getOrElse(2)
    println(t)
  }
  @Test
  def submit(): Unit = {
    val env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "sage-bigdata-etl/analyzer/streaming/target/sage-bigdata-etl-streaming-1.0.0-SNAPSHOT.jar")
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val orderA: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)))

    val orderB: DataStream[Order] = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))

    // register the DataStreams under the name "OrderA" and "OrderB"
    tEnv.registerDataStream("OrderA", orderA, 'user, 'product, 'amount)
    tEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount)

    // union the two tables
    val result = tEnv.sqlQuery(
      "SELECT * FROM OrderA WHERE amount > 2 UNION ALL " +
        "SELECT * FROM OrderB WHERE amount < 2")

    result.toAppendStream[Order].print()

    env.execute()
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(CONF.getInt("flink.parallelism"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    //  StreamITCase.testResults = mutable.MutableList()

    val stream: DataStream[(Long, String, Int, Long)] = env.fromCollection(Seq(
      (2L, "pen", 3, new Date().getTime),
      (2L, "rubber", 4, new Date().getTime),
      (4L, "beer", 1, new Date().getTime)))
    stream.assignAscendingTimestamps(_._4)

    val types = new RowTypeInfo(Array[TypeInformation[_]](
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO), Array[String](
      "id",
      "user",
      "amount",
      "rowtime"
    ))
    val rows: DataStream[Row] = stream.assignTimestampsAndWatermarks(TimeLagWatermarkGenerator()).map(
      event => {
        val row = new Row(4)

        row.setField(0, event._1)
        row.setField(1, event._2)
        row.setField(2, event._3)
        row.setField(3, event._4)
        row
      })(types)

    val table = rows.toTable(tEnv, Array[Expression](UnresolvedFieldReference("id"),
      UnresolvedFieldReference("user"),
      UnresolvedFieldReference("amount"),
      RowtimeAttribute(UnresolvedFieldReference("rowtime")),
      ProctimeAttribute(UnresolvedFieldReference("proctime"))).toSeq: _*)
    // val table = tEnv.fromDataStream(rows)
    tEnv.registerTable("aaa", table)


    val tablea = tEnv.sqlQuery("SELECT user, SUM(amount) FROM aaa GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user")
    //  tEnv.explain(tablea)
    tablea.addSink(da => println(da))
    env.execute()


  }

  import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
  import org.apache.flink.streaming.api.watermark.Watermark

  case class TimeLagWatermarkGenerator() extends AssignerWithPeriodicWatermarks[(Long, String, Int, Long)] {
    final private val maxTimeLag = 5000 // 5 seconds

    override def extractTimestamp(element: (Long, String, Int, Long), previousElementTimestamp: Long): Long = element._4

    override def getCurrentWatermark: Watermark = { // return the watermark as current time minus the maximum time lag
      new Watermark(System.currentTimeMillis - maxTimeLag)
    }
  }

}


case class Order(user: Long, product: String, amount: Int)