package com.haima.sage.bigdata.etl.plugin.es_6

import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import org.junit.Test

import scala.util.{Failure, Success}

class ElasticsearchClientTest {

  @Test
  def queryTest: Unit = {

    val client = ElasticClient("my_es_cluster", Array(("bigdata-dev06", 9200)))


    client.search(new SearchRequest(Array("zhangshuyu_*"), new SearchSourceBuilder()
      .sort("id", SortOrder.ASC)
      .query(QueryBuilders.wrapperQuery("{\n  \"bool\": {\n    \"must\": [\n      {\n        \"term\": {\n          \"id\": \"2\"\n        }\n      }\n    ]\n  }\n}"))
      .size(100).timeout(TimeValue.timeValueMinutes(1)) //how many hits per shard will be returned for each scroll

      .explain(true)).types("test").scroll(new TimeValue(60000))) match {
      case Success(re) =>
        re.getHits.getHits.foreach {
          host =>
            println(host.getSourceAsMap)
        }
      case Failure(exception)=>

    }


    client.free()
  }

}
