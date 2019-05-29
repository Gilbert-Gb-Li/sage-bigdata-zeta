package com.haima.sage.bigdata.etl.udf

import java.util.Date

import com.haima.sage.bigdata.analyzer.modeling.ModelingSQLAnalyzer
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.AnalyzerParser
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.junit.Test

class UdfTest {
  private val env = ExecutionEnvironment.getExecutionEnvironment
  Constants.init("sage-modeling.conf")
  val date = new Date().getTime


  protected val data: DataSet[RichMap] = env.fromElements(
    Map("a" -> "x", "b" -> 11l, "eventTime" -> new Date(date + 1 * 1000)),
    Map("a" -> "x", "b" -> 12l, "eventTime" -> new Date(date + 2 * 1000)),
    Map("a" -> "x", "b" -> 13l, "eventTime" -> new Date(date + 3 * 1000)),
    Map("a" -> "2", "b" -> 14l, "eventTime" -> new Date(date + 4 * 1000)),
    Map("a" -> "2", "b" -> 15l, "eventTime" -> new Date(date + 5 * 1000)),
    Map("a" -> "2", "b" -> 16l, "eventTime" -> new Date(date + 6 * 1000)),
    Map("a" -> "3", "b" -> 20l, "eventTime" -> new Date(date + 7 * 1000)),
    Map("a" -> "3", "b" -> 21l, "eventTime" -> new Date(date + 8 * 1000)),
    Map("a" -> "3", "b" -> 22l, "eventTime" -> new Date(date + 9 * 1000)),
    Map("a" -> "中国", "b" -> 22l, "eventTime" -> new Date(date + 10 * 1000)))

  val table = Table("", "tb1", Array(("a", "string"), ("b", "long")))

  @Test
  def selectAll(): Unit = {
    val conf = SQLAnalyzer("select a,PercentRank(b,CAST(0.99 as DOUBLE)) from tb1 group by a", filter = Array(AnalyzerParser(Some(NothingParser()))))

    val processor = new ModelingSQLAnalyzer(conf)
    assert(processor.engine() == AnalyzerModel.MODELING)
    val env = processor.getEnvironment(data)
    processor.register(data, table)(env)
    val rt = processor.analyze(env).collect()
    rt.foreach(println)
    assert(rt.size == 4)


  }
}
