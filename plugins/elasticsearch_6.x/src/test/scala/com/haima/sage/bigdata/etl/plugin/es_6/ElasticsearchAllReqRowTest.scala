package com.haima.sage.bigdata.etl.plugin.es_6

import com.haima.sage.bigdata.analyzer.sql.side.SideInfo
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import com.haima.sage.bigdata.etl.plugin.es_6.flink.side.ElasticsearchAllReqRow
import org.apache.calcite.sql.JoinType
import org.junit.Test

class ElasticsearchAllReqRowTest {

  @Test
  def loadTest(): Unit = {
    val source: ES6Source = ES6Source(
      "my_es_cluster",
      Array(("172.16.208.72", 9200)),
      "zhangshuyu",
      "test",
      "id",
      0,
      10000,
      2,
      None
    )

    val sideInfo: SideInfo = SideInfo(
      List(2),
      List("ID"),
      Map("ID" -> "id"),
      Map(5 -> 1, 4 -> 0),
      Map(2 -> 2, 1 -> 1, 3 -> 3, 0 -> 0),
      Map(5 -> "ID", 4 -> "DEPARTMENT"),
      List("ID", "DEPARTMENT"),
      JoinType.LEFT
    )

    val allReqRow: ElasticsearchAllReqRow = new ElasticsearchAllReqRow(source, AllSideTable(180), sideInfo)

    allReqRow.cluster(ElasticClient(source.cluster, source.hostPorts))
    allReqRow.loadData()
  }

}
