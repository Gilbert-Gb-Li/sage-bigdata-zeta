package com.haima.sage.bigdata.etl.es_5.tools

import com.haima.sage.bigdata.etl.common.model._

/**
  * Created by evan on 17-9-29.
  */
object ModelingConfigTest {

  def hdfsSource = HDFSSource(
    host = ConstantsTest.HDFS_HOST,
    port = ConstantsTest.HDFS_PORT,
    path = FileSource(
      path = ConstantsTest.HDFS_INPUT_PATH
    )
  )

  def es5Source = ES5Source(
    cluster = "zdp_es_cluster",
    hostPorts = Array((ConstantsTest.ES_HOST, ConstantsTest.ES_PORT)),
    index = ConstantsTest.ES_INDEX,
    esType = ConstantsTest.ES_TYPE,
    field = "",
    start = "",
    step = 100,
    queryDSL = None
  )

  /*def analyzerWithHDFS = SQLAnalyzer(
    parserId = "",
    fromFields = Array(("name", "string"), ("age", "int")),
    table = "modeling_table",
    sql = "select name, age from modeling_table where age < 33",
    timeField = None,
    idleStateRetentionTime = None,
    filter = Array(),
    metadata = None
  )

  def analyzerWithES5 = SQLAnalyzer(
    parserId = "",
    fromFields = Array(("name", "string"), ("age", "long")),
    table = "modeling_table",
    sql = "select name, age from modeling_table where age > 35",
    timeField = None,
    idleStateRetentionTime = None,
    filter = Array(),
    metadata = None
  )*/

  def tableWithHDFS = Table(
    parserId = "1",
    fields = Array(("name", "string"), ("age", "int")),
    tableName = "table_hdfs"
  )

  def tableWithES5 = Table(
    parserId = "2",
    fields = Array(("name", "string"), ("age", "int")),
    tableName = "table_es5"
  )

  def localFileWriter = FileWriter(
    path = ConstantsTest.WRITER_PATH
  )
}
