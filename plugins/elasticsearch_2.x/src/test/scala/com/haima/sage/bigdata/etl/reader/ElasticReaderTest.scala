package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.plugin.es_2.client.ElasticClient
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.junit.Test

/**
  * Created by zhhuiyan on 2016/11/16.
  */
class ElasticReaderTest {
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

}
