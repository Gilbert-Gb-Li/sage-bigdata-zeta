package com.haima.sage.bigdata.etl.plugin.es_6.connectors

import java.util.concurrent.atomic.AtomicInteger

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{ES6Source, RichMap}
import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.InputSplitAssigner
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

import scala.annotation.tailrec
import scala.util.{Failure, Success}

@SerialVersionUID(1L)
class ElasticseachInputFormat(conf: ES6Source) extends InputFormat[RichMap, ElasticsearchInputSplit] {

  @transient
  private var client: ElasticClient = _
  private var scrollId: String = _
  private var hits: Array[SearchHit] = _
  private val nextRecordIndex = new AtomicInteger(0)
  private lazy val step = if (conf.step > 10000) 10000 else conf.step.toInt


  override def createInputSplits(minNumSplits: Int): Array[ElasticsearchInputSplit] = {
    /*
    * now only one connector
    * */
    Array(ElasticsearchInputSplit(conf.index, conf.esType, conf.cluster, 0, conf.hostPorts(0)._1, null, conf.hostPorts(0)._2))
  }

  override def getInputSplitAssigner(inputSplits: Array[ElasticsearchInputSplit]): InputSplitAssigner = {
    new InputSplitAssigner() {
      var i: Int = -1

      override def getNextInputSplit(host: String, taskId: Int): ElasticsearchInputSplit =
        if (i >= inputSplits.length - 1)
          null
        else {
          i += 1
          inputSplits(i)
        }
    }
  }

  override def getStatistics(cachedStatistics: BaseStatistics): Null = {
    null
  }


  override def configure(conf: Configuration): Unit = {


  }


  override def open(split: ElasticsearchInputSplit): Unit = {
    client = split.driver.driver().get

    client.search(new SearchRequest(Array(conf.index), new SearchSourceBuilder().size(step) //size must be less than or equal to: [10000]
      .sort(conf.field, SortOrder.ASC)
      .query(QueryBuilders.rangeQuery(conf.field)
        .from(conf.start)
        .includeLower(false)
        .includeUpper(true))


    ).scroll(new TimeValue(600000))) match {
      case Success(searchResponse) =>
        scrollId = searchResponse.getScrollId
        hits = searchResponse.getHits.getHits()
      case Failure(e: NoNodeAvailableException) =>
        closeNow()
        throw e
      case Failure(e) =>
        throw e


    }

  }


  override def nextRecord(reuse: RichMap): RichMap = {
    val hit = hits(nextRecordIndex.getAndIncrement())
    import scala.collection.JavaConversions._
    hit.getSourceAsMap.toMap
  }

  @tailrec
  override final def reachedEnd(): Boolean = {
    if (hits.length <= 0) {
      true
    } else {
      if (nextRecordIndex.get() >= hits.length) {
        nextScroll()
        reachedEnd()
      } else {
        false
      }
    }
  }

  override def close(): Unit = {
    try {
      if (client != null) {
        client.free()
      }

    } finally {
      client = null
      scrollId = null
      hits = null
    }
  }


  private def nextScroll(): Unit = {
    client.scroll(scrollId, 600000) match {
      case Success(searchResponse) =>
        scrollId = searchResponse.getScrollId
        hits = searchResponse.getHits.getHits
        nextRecordIndex.set(0)
      case Failure(e: NoNodeAvailableException) =>
        closeNow()
        throw e
      case Failure(e) =>
        throw e
    }
  }


  private def closeNow(): Unit = {
    try {
      if (client != null) {
        client.freeNow()
      }
    } finally {
      client = null
      scrollId = null
      hits = null
    }
  }

}
