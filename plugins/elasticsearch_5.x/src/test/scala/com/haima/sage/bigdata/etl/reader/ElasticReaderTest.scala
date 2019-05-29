package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.ES5Source
import com.haima.sage.bigdata.etl.plugin.es_5.connectors.{ElasticseachInputFormat, ElasticsearchDataSet}
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.Path
import org.junit.Test

/**
  * Created by zhhuiyan on 2016/11/16.
  */
class ElasticReaderTest {

  @Test
  def esinputformat(): Unit ={
    val es = ES5Source(
      cluster = "cli",
      hostPorts = Array(("10.10.100.238",9200)),
      index = "logs_20180308",
      esType = "log",
      field = "content",
      start = "0",
      step = 1000,
      queryDSL = None)
//    val em = new ElasticseachInputFormat(es)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val s1 = System.currentTimeMillis()
    val set = ElasticsearchDataSet.fromElasticsearchQuery(env,es)

    println("set:"+set.count())
    val s2 = System.currentTimeMillis()
    println("time:"+(s2-s1))
  }

  @Test
  def read(): Unit = {

    /*val client = ElasticSearchClient.apply("elasticsearch", Array(("127.0.0.1",9200)))
    val builder: SearchRequestBuilder = client.prepareSearch("logs_")
    builder.setTypes("logs")
    builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
    val response: SearchResponse = builder.setSize(10).setExplain(true).execute.actionGet
    val hits = response.getHits


    hits.getHits.foreach(hit => {
      println(hit.getSource.get("field15"))
      println(hit.getSource.get("field15").getClass)
      println(hit.getSource.get("timestamp"))
      println(hit.getSource.get("timestamp").getClass)

    })*/

  }

  @Test
  def es5ReaderTest: Unit = {
    def es = ES5Source(
      cluster = "zdp_es_cluster",
      hostPorts = Array(("10.10.100.71",9200)),
      index = "bank",
      esType = "account",
      field = "",
      start = "",
      step = 100,
      queryDSL = None
    )

    val file = "file:///opt/aaa"

    def localOutput(file: String) = new TextOutputFormat[RichMap](new Path(file))

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(CONF.getInt("flink.parallelism"))
    val ds = ElasticsearchDataSet.fromElasticsearchQuery(
      env, es
    )

    //    ds.collect().foreach(println(_))
    ds.output(localOutput(file))
    env.execute("es5ReaderTest")

  }

  //  @Test
  //  def es5ReaderTest: Unit = {
  //  val file = s"${ConstantsTest.OUTPUT_PATH}/${filename("es5ReaderTest")}.tmp"
  //  val env = getEnv()
  //  new FlinkES5ModelingReader(ModelingConfigTest.es5Source).getDataSet(env).output(localOutput(file))
  //  env.execute("es5ReaderTest")
  //  assertOutput(file)
  //
  //  }

}
