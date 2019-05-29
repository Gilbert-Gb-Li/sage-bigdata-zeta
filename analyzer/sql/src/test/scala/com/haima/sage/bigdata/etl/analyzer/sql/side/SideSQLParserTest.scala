package com.haima.sage.bigdata.analyzer.sql.side

import com.dtstack.flink.sql.side.SideSQLParser
import org.junit.Test

class SideSQLParserTest {

  @Test
  def parserSQL(): Unit = {
    val parser = new SideSQLParser()
    val sql = "select n.*, d.department from name AS n left join department AS d on n.d_id = d.id"
    import scala.collection.JavaConversions._
    parser.getExeQueue(sql, List("DEPARTMENT"))
  }

}
