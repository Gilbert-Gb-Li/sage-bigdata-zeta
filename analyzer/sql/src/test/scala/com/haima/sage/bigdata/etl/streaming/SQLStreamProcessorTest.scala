package com.haima.sage.bigdata.etl.streaming

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.analyzer.streaming.{StreamJoinAnalyzer, StreamSQLAnalyzer}
import com.haima.sage.bigdata.analyzer.streaming.filter.StreamAnalyzerProcessor
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.junit.Test

import scala.io.Source


class SQLStreamProcessorTest {


  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(CONF.getInt("flink.parallelism"))
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  Constants.init("sage-streaming.conf")

  val time = new Date().getTime - 100000
  protected val data: DataStream[RichMap] = env.fromElements(
    Map("a" -> "x", "b" -> 15, "eventTime" -> time),
    Map("a" -> "x", "b" -> 15, "eventTime" -> (time + 1000)),
    Map("a" -> "x", "b" -> 16, "eventTime" -> (time + 2 * 1000)),
    Map("a" -> "y", "b" -> 15, "eventTime" -> (time + 3 * 1000)),
    Map("a" -> "y", "b" -> 16, "eventTime" -> (time + 4 * 1000)),
    Map("a" -> "y", "b" -> 17, "eventTime" -> (time + 5 * 1000)),
    Map("a" -> "z", "b" -> 20, "eventTime" -> (time + 6 * 1000)),
    Map("a" -> "z", "b" -> 21, "eventTime" -> (time + 7 * 1000)),
    Map("a" -> "z", "b" -> 22, "eventTime" -> (time + 8 * 1000)))

  protected val data2: DataStream[RichMap] = env.fromElements(
    Map("a2" -> "x", "b" -> 15, "eventTime" -> time),
    Map("a2" -> "x", "b" -> 15, "eventTime" -> (time + 1 * 1000)),
    Map("a2" -> "x", "b" -> 16, "eventTime" -> (time + 2 * 1000)),
    Map("a2" -> "y", "b" -> 15, "eventTime" -> (time + 3 * 1000)),
    Map("a2" -> "y", "b" -> 16, "eventTime" -> (time + 4 * 1000)),
    Map("a2" -> "y", "b" -> 17, "eventTime" -> (time + 5 * 1000)),
    Map("a2" -> "z", "b" -> 20, "eventTime" -> (time + 6 * 1000)),
    Map("a2" -> "z", "b" -> 21, "eventTime" -> (time + 7 * 1000)),
    Map("a2" -> "z", "b" -> 22, "eventTime" -> (time + 8 * 1000)))


  @Test
  def joinThenSQL(): Unit = {

    val analyzer = SQLAnalyzer("select * from tb1", filter = Array(AnalyzerParser(Some(NothingParser()))))
    val handle = new StreamJoinAnalyzer(Join(JoinConfig("", "b", EventTime("eventTime", 2000L)),
      JoinConfig("", "b", EventTime("eventTime", 2000L)), window = TumblingWindow(1000, EventTime("eventTime"))))

    val data3 = handle.join(data, data2)


    val handle2 = new StreamSQLAnalyzer(analyzer)

    implicit val env2 = handle2.getEnvironment(data3)
    handle2.register(data3, Table("", "tb1",
      Array(("a", "string"), ("a2", "string"), ("b", "integer"), ("eventTime", "long"))))
    handle2.analyze(env2).print()
    env.execute()

  }

  @Test
  def joinThenSQL2: Unit = {


    val data: DataStream[RichMap] = env.readTextFile("/Users/zhhuiyan/Downloads/timeseries-log.txt").map(d =>
      new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
        case ("@timestamp", value: String) =>
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          ("@timestamp", format.parse(value).getTime)
        case (key, value: Map[_, _]) =>
          (key, "")

        case (key, value) =>
          (key, value)

      })
    val data2: DataStream[RichMap] = env.readTextFile("/Users/zhhuiyan/Downloads/timeseries-log_forecast.txt").map(d =>
      new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
        case ("@timestamp", value: String) =>
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          ("@timestamp", format.parse(value).getTime)
        case (key, value: Map[_, _]) =>
          (key, "")
        case (key, value) =>
          (key, value)

      })
    val join = Join(JoinConfig("", "@timestamp", EventTime("@timestamp")),
      JoinConfig("", "@timestamp", EventTime("@timestamp")), window = TumblingWindow(12000, EventTime("eventTime")))
    val data3 = new StreamJoinAnalyzer(join).join(data, data2)

    // data3.print()
    val analyzer = SQLAnalyzer("select * from tb1", filter = Array(AnalyzerParser(Some(NothingParser()))))
    val handle2 = new StreamSQLAnalyzer(analyzer)

    implicit val env2 = handle2.getEnvironment(data3)
    handle2.register(data3, Table("", "tb1",
      Array(("@timestamp", "long"), ("usage_user", "string"), ("usage_user_forecast", "double"), ("endpoint_id", "string"))
      , EventTime("@timestamp")
    ))
    handle2.analyze(env2).print()
    env.execute()
    TimeUnit.SECONDS.sleep(1)

  }

  // implicit val system = ActorSystemFactory.get("test",host = "10.20.5.128",port = 19999);

  @Test
  def selectAll(): Unit = {
    val table = Table("",
      "tb1", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))

    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1", Some(table), filter = Array(AnalyzerParser(Some(NothingParser()))))
    ))
    val processor = StreamAnalyzerProcessor(conf)
    assert(processor.engine() == AnalyzerModel.STREAMING)
    processor.process(data).head.map(data => {
      (data.get("a").orNull, data.get("b").orNull, data.get("eventTime").orNull)
    }).writeAsCsv("./target/aa.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)
    assert(Source.fromFile("./target/aa.txt").getLines().count(_ => true) == 9)

  }

  @Test
  def selectX(): Unit = {
    val table = Table("",
      "tb1", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))
    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1 where a='x'",
      table = Some(table),
      filter = Array(AnalyzerParser(Some(NothingParser()))))
    ), None)

    val processor = StreamAnalyzerProcessor(conf)
    processor.process(data).head.map(data => {
      (data.get("a").orNull, data.get("b").orNull, data.get("eventTime").orNull)
    }).
      writeAsCsv("./target/aa.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)
    assert(Source.fromFile("./target/aa.txt").getLines().count(_ => true) == 3)


  }

  @Test
  def useParserFilter(): Unit = {
    val table = Table("",
      "tb1", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))
    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1 where a='x'",
      table = Some(table),
      filter = Array(AnalyzerParser(Some(NothingParser(filter = Array(AddFields(Map("c" -> "5"))))))))
    ), None)

    val processor = StreamAnalyzerProcessor(conf)

    processor.process(data).head.map(data => {
      (data.get("a").orNull, data.get("b").orNull, data.get("c").orNull, data.get("eventTime").orNull)
    }).
      writeAsCsv("./target/aa.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)
    assert(Source.fromFile("./target/aa.txt").getLines().map(line => {
      line.split(",")(2)
    }).filter(_.matches("\\d+")).map(_.toLong).sum == 15)
    //  assert(processor.process(data).head.collect().map(_.getOrElse("c", "0").asInstanceOf[String].toInt).sum == 15)

  }

  @Test
  def switchCaseNoDefault(): Unit = {
    val table2 = Table("",
      "tb2", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))

    val table = Table("",
      "tb1", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))
    val sub = ReAnalyzer(Some(SQLAnalyzer("select sum(b) as c from tb2 ",
      table = Some(table2)
    )), None)
    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1",
      table = Some(table),
      filter = Array(AnalyzerContain("a", Array(AnalyzerCase("x", sub)))))
    ), None)

    val processor = StreamAnalyzerProcessor(conf)
    val rt = processor.process(data)
    assert(rt.lengthCompare(1) == 0)
    rt.head.map(data => {
      data.get("c").orNull
    }).
      writeAsText("./target/aa.txt", FileSystem.WriteMode.OVERWRITE)

    env.execute()
    TimeUnit.SECONDS.sleep(1)
    assert(Source.fromFile("./target/aa.txt").getLines().filter(_.matches("\\d+")).toList.last.toLong == 46)
  }


  @Test
  def switchCaseWithDefaultNoWindow(): Unit = {
    val table2 = Table("",
      "tb2", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))

    val table = Table("",
      "tb1", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))

    val sub = ReAnalyzer(Some(SQLAnalyzer("select sum(b) as c from tb2 ",
      table = Some(table2))), None)
    val sub2 = ReAnalyzer(Some(SQLAnalyzer("select count(b) as d from tb2 ", table = Some(table2))
    ), None)
    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1",
      table = Some(table),
      filter = Array(AnalyzerContain("a", Array(AnalyzerCase("x", sub)), Some(sub2))))
    ), None)

    val processor = StreamAnalyzerProcessor(conf)

    val rt = processor.process(data)
    assert(rt.size == 2)
    rt.zipWithIndex.foreach {
      case (stream, index) => {
        stream.map(_.getOrElse("c", 0)).writeAsText(s"./target/$index-c.txt", FileSystem.WriteMode.OVERWRITE)
        stream.map(_.getOrElse("d", 0)).writeAsText(s"./target/$index-d.txt", FileSystem.WriteMode.OVERWRITE)
      }
    }
    env.execute()
    TimeUnit.SECONDS.sleep(1)
    assert((0 to 1).forall(index => {
      Source.fromFile(s"./target/$index-c.txt").getLines().size > 1
    }))


    assert((0 to 1).exists(index => {
      Source.fromFile(s"./target/$index-c.txt").getLines().filter(_.matches("\\d+")).map(_.toLong).toList.max == 46
    }))

    assert((0 to 1).exists(index => {
      Source.fromFile(s"./target/$index-d.txt").getLines().filter(_.matches("\\d+")).map(_.toLong).toList.max == 6
    }))

  }

  @Test
  def sqlThenJoin: Unit = {


    val table = Table("",
      "tb1", Array(("@timestamp", "date"), ("endpoint_id", "string"), ("usage_user", "double")), EventTime("@timestamp"))
    val ts1 = env.readTextFile("/Users/zhhuiyan/Downloads/ts-1.txt").map(d => RichMap(new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
      case (key, value: String) if key == "@timestamp" =>
        val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value)
        println("class:" + d.getClass)
        (key, d)
      case (key, value) =>
        (key, value)
    }
    ))
    val ts2 = env.readTextFile("/Users/zhhuiyan/Downloads/ts-2.txt").map(d => RichMap(new Mapper {}.mapper.readValue[Map[String, Any]](d).map {
      case (key, value: String) if key == "newtime" =>
        val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value)

        println("class:" + d.getClass)
        (key, d)
      case (key, value) =>
        (key, value)
    }))
    val sql1 = new StreamSQLAnalyzer(SQLAnalyzer("select * from tb1",
      table = Some(table)
    ))
    val ts1_sql = sql1.action(ts1)
    /*.map(d => {
          RichMap(d.map {
            case (key, value: Date) if key == "@timestamp" =>

              (key, new Date(value.getTime + 8 * 60 * 60 * 1000))
            case (key, value) =>
              (key, value)
          })


        })*/
    val join = Join(JoinConfig("", "endpoint_id", EventTime("@timestamp", 10000L)),
      JoinConfig("", "endpoint_id", EventTime("newtime", 10000L)), None, TumblingWindow(10 * 60 * 1000, EventTime("newtime", 10000L)))
    ts1_sql.writeAsText("./target/out1.txt", FileSystem.WriteMode.OVERWRITE)
    ts2.writeAsText("./target/out2.txt", FileSystem.WriteMode.OVERWRITE)
    new StreamJoinAnalyzer(join).join(ts1_sql, ts2).writeAsText("./target/out.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)


  }


  @Test
  def switchCaseWithDefaultEventWindow(): Unit = {
    val table2 = Table("",
      "tb2", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))

    val table = Table("",
      "tb1", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")), EventTime("eventTime"))

    val sub = ReAnalyzer(Some(SQLAnalyzer("select sum(b) as c from tb2 GROUP BY TUMBLE(eventTime, INTERVAL '1' SECOND)",
      table = Some(table2)
    )), None)
    val sub2 = ReAnalyzer(Some(SQLAnalyzer("select count(b) as d from tb2 GROUP BY TUMBLE(eventTime, INTERVAL '1' SECOND)",
      table = Some(table2)
    )), None)
    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1 ",
      table = Some(table),
      filter = Array(AnalyzerContain("a", Array(AnalyzerCase("x", sub)), Some(sub2))))
    ), None)

    val processor = StreamAnalyzerProcessor(conf)

    val rt = processor.process(data)
    assert(rt.size == 2)
    rt.zipWithIndex.foreach {
      case (stream, index) =>
        stream.map(_.getOrElse("c", 0)).writeAsText(s"./target/$index-c.txt", FileSystem.WriteMode.OVERWRITE)
        stream.map(_.getOrElse("d", 0)).writeAsText(s"./target/$index-d.txt", FileSystem.WriteMode.OVERWRITE)
    }
    env.execute()
    TimeUnit.SECONDS.sleep(1)

    //    assert((0 to 1).forall(index => {
    //      Source.fromFile(s"./target/$index-c.txt").getLines().size > 1
    //    }))
    //    assert((0 to 1).exists(index => {
    //      Source.fromFile(s"./target/$index-c.txt").getLines().filter(_.matches("\\d+")).map(_.toLong).toList.max == 16
    //    }))
    //
    //    assert((0 to 1).exists(index => {
    //      Source.fromFile(s"./target/$index-d.txt").getLines().filter(_.matches("\\d+")).map(_.toLong).toList.max == 6
    //    }))

  }

}
