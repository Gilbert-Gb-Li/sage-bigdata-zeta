package com.haima.sage.bigdata.etl.plugin.es_6.flink.side


import java.sql.Timestamp
import java.util.concurrent.{Executors, TimeUnit}
import java.util.{Date, function}

import com.haima.sage.bigdata.analyzer.sql.side.{AllReqRow, SideInfo}
import com.haima.sage.bigdata.etl.common.model.{AllSideTable, ES6Source}
import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.calcite.sql.JoinType
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

@SerialVersionUID(1L)
class ElasticsearchAllReqRow(override val dataSource: ES6Source, override val table: AllSideTable, override val sideInfo: SideInfo) extends AllReqRow with Logger {

  @transient
  private var client: ElasticClient = _

  private lazy val step = if (dataSource.step > 10000) 10000 else dataSource.step.toInt

  override def fillData(input: Row, sideInput: Any): Row = {
    val out = new Row(sideInfo.inFieldIndex.size + sideInfo.sideFieldNameIndex.size)

    sideInfo.inFieldIndex.foreach {
      case (k, v) =>
        out.setField(
          k,
          input.getField(v) match {
            case t: Timestamp => t.getTime
            case o => o
          }
        )
    }
    sideInput match {
      case si: Map[String@unchecked, Any@unchecked] =>
        sideInfo.sideFieldNameIndex.foreach {
          case (k, v) =>
            out.setField(k, si.getOrElse(v, null))
        }
      case _ =>
        sideInfo.sideFieldNameIndex.foreach {
          case (k, _) =>
            out.setField(k, null)
        }
    }

    out
  }

  override def flatMap(in: Row, collector: Collector[Row]): Unit = {
    val inputParams = mutable.ListBuffer[Any]()
    sideInfo.equalValIndex.foreach {
      i =>
        val v = in.getField(i)
        //        if (v == null) collector.collect(null)
        inputParams.append(v)
    }
    val key = buildKey(inputParams.toList)
    val cacheList = cache.get(key)

    if (cacheList.nonEmpty) {
      cacheList.get.foreach(one => collector.collect(fillData(in, one)))
    } else {
      if (sideInfo.joinType == JoinType.LEFT) {
        val row = fillData(in, null)
        collector.collect(row)
      }
    }

  }

  def loadData(): Unit = {
    logger.info("Loading data...")
    try {
      var hitsTuple = getHits(None)
      var next = true
      while (next) {
        if (hitsTuple._2.nonEmpty) {
          cacheData(hitsTuple._2, sideInfo.equalFieldList)
          hitsTuple = getHits(Option(hitsTuple._1))
        } else {
          next = false
        }
      }
    } catch {
      case ex: Throwable =>
        logger.error("load data error", ex)
    }
    logger.info(s"Load data finished, cache ${cacheSize.get()} total.")
  }

  def getHits(scrollId: Option[String]): (String, Array[SearchHit]) = {
    scrollId match {
      case Some(id) =>
        client.scroll(id, 60000) match {
          case Success(searchRes) =>
            (searchRes.getScrollId, searchRes.getHits.getHits)
          case Failure(exception) =>
            throw exception
        }

      case None =>
        val queries = dataSource.queryDSL match {
          case Some(queryStr) if StringUtils.isNotEmpty(queryStr.trim) =>
            QueryBuilders.boolQuery()
              .must(QueryBuilders.wrapperQuery(queryStr))
              .must(QueryBuilders.rangeQuery(dataSource.field)
                .from(dataSource.start match {
                  case d: Date =>
                    d.getTime
                  case d => d
                }).includeLower(false)
                .includeUpper(true))
          case _ =>
            QueryBuilders.rangeQuery(dataSource.field)
              .from(dataSource.start match {
                case d: Date =>
                  d.getTime
                case d => d
              })
              .includeLower(false)
              .includeUpper(true)
        }

        val source = new SearchSourceBuilder()
          .query(queries)
          .size(step)
          .sort(dataSource.field, SortOrder.ASC)

        val req=new SearchRequest(Array(dataSource.index), source)
          .types(dataSource.esType).scroll(new TimeValue(600000))
        client.search(req) match {
          case Success(searchResponse) =>
            (searchResponse.getScrollId, searchResponse.getHits.getHits())
          case Failure(e) =>
            throw e;

        }


    }
  }

  def cacheData(hits: Array[SearchHit], joinFields: List[String]): Unit = {
    hits.foreach {
      hit =>
        import scala.collection.JavaConversions._
        val data = hit.getSourceAsMap.toMap
        val key = buildKey(data, joinFields)
        cache.computeIfAbsent(key, new function.Function[String, mutable.ListBuffer[Map[String, Any]]] {
          override def apply(t: String): ListBuffer[Map[String, Any]] = mutable.ListBuffer()
        }).append(data)
    }
    logger.info(s"Cache ${cacheSize.addAndGet(hits.length)} data so far.")
  }

  /**
    * build cache key
    *
    * @param fieldValues join field values List
    * @return
    */
  def buildKey(fieldValues: List[Any]): String = {
    val output = new StringBuilder()
    fieldValues.foreach {
      f =>
        if (output.nonEmpty)
          output.append("_").append(f)
        else
          output.append(f)
    }
    output.toString()
  }

  /**
    * build cache key
    *
    * @param data One piece of data
    * @return cache key
    */
  def buildKey(data: Map[String, Any], joinFields: List[String]): String = {
    val output = new StringBuilder()
    val tmpData = data.map(d => (d._1.toUpperCase, d._2))
    joinFields.foreach {
      f =>
        tmpData.get(f) match {
          case Some(s) =>
            if (output.nonEmpty)
              output.append("_").append(s)
            else
              output.append(s)
          case None =>

        }
    }
    output.toString()
  }

  override def close(): Unit = {
    if (client != null) {
      client.free()
    }
    super.close()
  }


  override def open(parameters: Configuration): Unit = {
    val service = Executors.newSingleThreadScheduledExecutor()
    client = ElasticClient(dataSource.cluster, dataSource.hostPorts)
    super.open(parameters)
    initCache()
    val runnable = new Runnable {
      override def run() = {
        reloadCache()
      }
    }
    service.scheduleWithFixedDelay(runnable, table.updateTime, table.updateTime, TimeUnit.SECONDS)
  }

  def cluster(_client: ElasticClient): Unit = {
    client = _client
  }
}
