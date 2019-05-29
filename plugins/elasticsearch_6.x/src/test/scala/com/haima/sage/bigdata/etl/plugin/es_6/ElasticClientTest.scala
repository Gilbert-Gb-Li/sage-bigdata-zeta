package com.haima.sage.bigdata.etl.plugin.es_6

import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import org.junit.Test

import scala.util.{Failure, Success}

class ElasticClientTest {

  val clusterName = "my_es_cluster"
  val hosts_port = Array(("bigdata-dev06", 9200))


  @Test
  def oneTest(): Unit = {
    val client = ElasticClient(clusterName, hosts_port)
    query(client)
    client.free()
  }

  @Test
  def queryTest(): Unit = {
    val client = ElasticClient(clusterName, hosts_port)
    val source = new SearchSourceBuilder()
      .query(
        QueryBuilders.boolQuery()
          .must(QueryBuilders.matchQuery("id", 3))
          .must(QueryBuilders.matchQuery("department", "成都"))
      )
      .size(100)
    client.search(
      new SearchRequest(Array("zhangshuyu_department"), source)
        .types("test").scroll(new TimeValue(600000))) match {
      case Success(result) =>
        result.getHits.getHits.foreach {
          d =>
            import scala.collection.JavaConversions._
            println(s"data = ${d.getSourceAsMap.toMap} ")
        }
      case Failure(exception)=>

    }

    client.free()
  }

  def query(client: ElasticClient, index: Int = 0): Unit = {
    val field = "id"
    val source = new SearchSourceBuilder()
      .query(
        QueryBuilders.rangeQuery(field)
          .from(0)
          .includeLower(false)
          .includeUpper(true)
      )
      .size(100)
      .sort(field, SortOrder.ASC)

    client.search(
      new SearchRequest(Array("zhangshuyu_department"), source)
        .types("test").scroll(new TimeValue(600000))) match {
      case Success(result) =>
        result.getHits.getHits.foreach {
          d =>
            import scala.collection.JavaConversions._
            println(s"XXX date = ${new Date()},index = $index, data = ${d.getSourceAsMap.toMap} ")
        }
      case Failure(exception)=>

    }


  }

  @Test
  def multiTest(): Unit = {

    (1 to 5).foreach {
      i =>
        val client = ElasticClient(clusterName, hosts_port)
        val service = Executors.newSingleThreadScheduledExecutor()
        val runnable = new Runnable {
          override def run() = {
            query(client, i)
            println(s"XXX ====================$i===================")
          }
        }
        service.scheduleWithFixedDelay(runnable, 5, 5, TimeUnit.SECONDS)

    }

    while (true) {
      Thread.sleep(60000)
    }

  }

}