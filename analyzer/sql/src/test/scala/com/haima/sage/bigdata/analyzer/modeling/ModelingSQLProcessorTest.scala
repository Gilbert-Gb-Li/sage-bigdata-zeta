package com.haima.sage.bigdata.analyzer.modeling

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.haima.sage.bigdata.analyzer.modeling.filter.ModelingAnalyzerProcessor
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.core.fs.FileSystem
import org.junit.Test


class ModelingSQLProcessorTest {


  private val env = ExecutionEnvironment.getExecutionEnvironment
  Constants.init("sage-modeling.conf")
  val date = new Date().getTime
  protected val data: DataSet[RichMap] = env.fromElements(
    Map("a" -> "x", "b" -> 15, "eventTime" -> new Date(date + 1 * 1000)),
    Map("a" -> "x", "b" -> 15, "eventTime" -> new Date(date + 2 * 1000)),
    Map("a" -> "x", "b" -> 16, "eventTime" -> new Date(date + 3 * 1000)),
    Map("a" -> "2", "b" -> 15, "eventTime" -> new Date(date + 4 * 1000)),
    Map("a" -> "2", "b" -> 16, "eventTime" -> new Date(date + 5 * 1000)),
    Map("a" -> "2", "b" -> 17, "eventTime" -> new Date(date + 6 * 1000)),
    Map("a" -> "3", "b" -> 20, "eventTime" -> new Date(date + 7 * 1000)),
    Map("a" -> "3", "b" -> 21, "eventTime" -> new Date(date + 8 * 1000)),
    Map("a" -> "3", "b" -> 22, "eventTime" -> new Date(date + 9 * 1000)),
    Map("a" -> "中国", "b" -> 22, "eventTime" -> new Date(date + 10 * 1000)))

  val table = Table("", "tb1", Array(("a", "string"), ("b", "integer")))

  @Test
  def selectAll(): Unit = {


    val conf = SQLAnalyzer("select * from tb1", filter = Array(AnalyzerParser(Some(NothingParser()))))

    val processor = new ModelingSQLAnalyzer(conf)
    assert(processor.engine() == AnalyzerModel.MODELING)
    val env = processor.getEnvironment(data)
    processor.register(data, table)(env)
    assert(processor.analyze(env).collect().size == 10)

  }

  @Test
  def sqlThenJoin: Unit = {


    val table = Table("",
      "tb1", Array(("@timestamp", "date"), ("endpoint_id", "string"), ("usage_user", "double")))
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
    val sql1 = new ModelingSQLAnalyzer(SQLAnalyzer("select * from tb1",
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
    val join = Join(JoinConfig("", "@timestamp", EventTime("@timestamp", 10000L)),
      JoinConfig("", "newtime", EventTime("newtime", 10000L)))
    ts1_sql.writeAsText("./target/out1.txt", FileSystem.WriteMode.OVERWRITE)
    ts2.writeAsText("./target/out2.txt", FileSystem.WriteMode.OVERWRITE)
    new ModelingJoinAnalyzer(join).join(ts1_sql, ts2).writeAsText("./target/out.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)


  }


  @Test
  def selectZh(): Unit = {
    val conf = SQLAnalyzer("select * from tb1 where a like '%国%'" /*, filter = Array(AnalyzerParser(Some(NothingParser())))*/)
    val processor = new ModelingSQLAnalyzer(conf)
    val env = processor.getEnvironment(data)
    processor.register(data, table)(env)
    assert(processor.analyze(env).collect().size == 1)

  }

  @Test
  def selectX(): Unit = {

    val conf = SQLAnalyzer("select * from tb1 where a='x'" /*, filter = Array(AnalyzerParser(Some(NothingParser())))*/)

    val processor = new ModelingSQLAnalyzer(conf)
    val env = processor.getEnvironment(data)
    processor.register(data, table)(env)
    assert(processor.analyze(env).collect().size == 3)

  }

  @Test
  def selectTUMBLE(): Unit = {
    /*
    SELECT TUMBLE_START(eventTime, INTERVAL '5' second) as wStart," +
        "TUMBLE_END(eventTime, INTERVAL '5' second) as wEnd," +
        "a," +
        "count(a)  as num FROM tb1 " +
        "GROUP BY TUMBLE(eventTime, INTERVAL '5' second),a
    */
    val table = Table("",
      "tb1",
      Array(("a", "string"), ("b", "integer"), ("eventTime", "datetime")),
      EventTime("eventTime"))

    val conf = ReAnalyzer(Some(SQLAnalyzer(
      "SELECT TUMBLE_START(eventTime, INTERVAL '5' second) as wStart," +
        "TUMBLE_END(eventTime, INTERVAL '5' second) as wEnd," +
        "a,count(b) as num FROM tb1 " +
        "GROUP BY TUMBLE(eventTime, INTERVAL '5' second),a"
      , table = Some(table)
    )
    ), None)

    val processor = ModelingAnalyzerProcessor(conf)

    processor.process(data).head.collect().foreach(println)
    // assert(processor.process(data).head.collect().size == 3)

  }

  /*@Test
  def selectTUMBLE2(): Unit = {
    /*
    SELECT TUMBLE_START(eventTime, INTERVAL '5' second) as wStart," +
        "TUMBLE_END(eventTime, INTERVAL '5' second) as wEnd," +
        "a," +
        "count(a)  as num FROM tb1 " +
        "GROUP BY TUMBLE(eventTime, INTERVAL '5' second),a
    */

    val conf = ReAnalyzer(Some(SQLAnalyzer("", Array(("a", "string"), ("b", "integer"), ("eventTime", "datetime")),
      "tb1", "SELECT TUMBLE_START(eventTime, INTERVAL '5' second) as wStart," +
        "TUMBLE_END(eventTime, INTERVAL '5' second) as wEnd," +
        "count(*)  as num FROM tb1 " +
        "GROUP BY TUMBLE(eventTime, INTERVAL '5' second)", "eventTime", Some("eventTime") /*, filter = Array(AnalyzerParser(Some(NothingParser())))*/)
    ), None)

    val processor = ModelingAnalyzerProcessor(conf)

    processor.process(data2).head.collect().foreach(println)
    // assert(processor.process(data).head.collect().size == 3)

  }*/

  @Test
  def useParserFilter(): Unit = {

    val table = Table("",
      "tb1", Array(("a", "string"), ("b", "integer"), ("eventTime", "long")))

    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1 where a='x'", table = Some(table),
      filter = Array(AnalyzerParser(Some(NothingParser(filter = Array(AddFields(Map("c" -> "5"))))))))
    ), None)

    val processor = ModelingAnalyzerProcessor(conf)


    assert(processor.process(data).head.collect().map(_.getOrElse("c", "0").asInstanceOf[String].toInt).sum == 15)

  }

  @Test
  def switchCaseNoDefault(): Unit = {
    val table1 = Table("",
      "tb2", Array(("a", "string"), ("b", "integer")))
    val table2 = Table("",
      "tb1", Array(("a", "string"), ("b", "integer")))
    val sub = ReAnalyzer(Some(SQLAnalyzer("select sum(b) as c from tb2 ", table = Some(table1))), None)
    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1", table = Some(table2),
      filter = Array(AnalyzerContain("a", Array(AnalyzerCase("x", sub)))))
    ), None)

    val processor = ModelingAnalyzerProcessor(conf)

    val rt = processor.process(data).map(set => {
      set.collect()
    })
    rt.foreach(println)
    assert(rt.size == 1)
    assert(rt.head.map(_.getOrElse("c", "0").asInstanceOf[Int]).sum == 46)

  }

  @Test
  def switchCaseWithDefault(): Unit = {
    val tb2 = Table("",
      "tb2", Array(("a", "string"), ("b", "integer")))
    val table2 = Table("",
      "tb1", Array(("a", "string"), ("b", "integer")))
    val sub = ReAnalyzer(Some(SQLAnalyzer("select sum(b) as c from tb2 ", table = Some(tb2))), None)
    val sub2 = ReAnalyzer(Some(SQLAnalyzer("select count(b) as d from tb2 ", table = Some(tb2))), None)
    val conf = ReAnalyzer(Some(SQLAnalyzer("select * from tb1", table = Some(table2),
      filter = Array(AnalyzerContain("a", Array(AnalyzerCase("x", sub)), Some(sub2))))
    ), None)

    val processor = ModelingAnalyzerProcessor(conf)

    val rt = processor.process(data).map(set => {
      set.collect()
    })
    assert(rt.size == 2)


    assert(rt.exists(_.map(_.getOrElse("c", 0).asInstanceOf[Int]).sum == 46))
    assert(rt.exists(_.map(_.getOrElse("d", 0l).asInstanceOf[Long]).sum == 7l))

  }
}
