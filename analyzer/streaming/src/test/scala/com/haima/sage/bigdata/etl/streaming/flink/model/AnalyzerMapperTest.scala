package com.haima.sage.bigdata.etl.streaming.flink.model

import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.common.model.{AnalyzerWrapper, SQLAnalyzer, Table}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

class AnalyzerMapperTest extends Mapper {


  @Test
  def data(): Unit = {
    val json =
      s"""{"id":"1","name":"test","data":{"first":{"parserId":"1","name":"table1","fields":[["id","string"],["a","long"]],"maxOutOfOrderness":10000},"second":{"parserId":"2","name":"table2","fields":[["id","string"],["b","long"]],"maxOutOfOrderness":10000},"sql":"select table1.a, table2.b from table1 inner join table2 on table1.id = table2.id","model":"streaming","filter":[],"name":"sql","parserId":""},"isSample":0}""".stripMargin

    val wrapper = mapper.readValue[AnalyzerWrapper](json)
    val firstTable = Table("1", "table1", Array(("id", "string"), ("a", "long")), null)
    val secondTable = Table("2", "table2", Array(("id", "string"), ("b", "long")), null)
    val analyzer = SQLAnalyzer("select table1.a, table2.b from table1 inner join table2 on table1.id = table2.id",
      filter = Array(AnalyzerRedirect("a", Array(AnalyzerCase("aa", ReAnalyzer(wrapper.data))))))

    println(mapper.writeValueAsString(analyzer))

    val json2 = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(wrapper.copy(data = Some(analyzer)))
    println(json2)
    println(mapper.readValue[AnalyzerWrapper](json2))

  }
}
