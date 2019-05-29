package com.haima.sage.bigdata.etl.plugin.es_6.flink.side

import java.sql.Timestamp

import com.dtstack.flink.sql.side.CacheMissVal
import com.dtstack.flink.sql.side.cache.{CacheObj, LRUSideCache}
import com.dtstack.flink.sql.side.enums.ECacheContentType
import com.haima.sage.bigdata.analyzer.sql.side.{AsyncReqRow, SideInfo}
import com.haima.sage.bigdata.etl.common.model.{AsyncSideTable, ES6Source}
import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.types.Row
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable
import scala.util.{Failure, Success}

@SerialVersionUID(1L)
class ElasticsearchAsyncReqRow(override val dataSource: ES6Source, override val table: AsyncSideTable, override val sideInfo: SideInfo) extends AsyncReqRow with Logger {

  @transient
  private var client: ElasticClient = _

  override def open(parameters: Configuration): Unit = {
    client = ElasticClient(dataSource.cluster, dataSource.hostPorts)

    sideCache = new LRUSideCache(table.cacheSize, table.expireTime)

    logger.info(s"Initializing sideCache( cacheSize=${table.cacheSize}, expireTime=${table.expireTime}s )")
    sideCache.initCache()
    super.open(parameters)
  }

  override def asyncInvoke(in: Row, resultFuture: ResultFuture[Row]): Unit = {
    import scala.collection.JavaConversions._
    // 找出字段值 和 对应的index
    val inputParams = mutable.ListBuffer[(Any, String)]()
    sideInfo.equalValIndex.zipWithIndex.foreach {
      i =>
        inputParams.append((in.getField(i._1), sideInfo.equalFieldList(i._2)))
    }

    // Join的字段为null时，不解析Join操作
    if (inputParams.exists(_._1 == null)) {
      dealMissKey(in, resultFuture)
    } else {
      val key = buildKey(inputParams.toList)
      if (openCache()) {
        // 先查询缓存中是否存在
        val obj = getFromCache(key)
        if (obj != null) {
          // 缓存中存在
          obj.getType match {
            case ECacheContentType.MissVal => // 缓存类型为为命中
              dealMissKey(in, resultFuture)
            case ECacheContentType.MultiLine => // 缓存类型为多行
              val out = obj.getContent.asInstanceOf[List[Map[String, Any]]].map(fillData(in, _))
              resultFuture.complete(out)
            case _ =>
              logger.warn(s"UnSupport cache type ${obj.getType}.")
              dealMissKey(in, resultFuture)
          }

        } else {
          // 缓存中不存在，则进行es查询
          val data = queryElasticsearch(inputParams.toList)
          if (data.nonEmpty) // 命中结果，进行Join操作，并将结果加入到缓存中
          {
            val out = data.map(fillData(in, _))
            resultFuture.complete(out)
            if (openCache()) {
              putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, data))
            }
          }
          else // 没有命中结果，将查询条件标示为未命中，加入到缓存中
          {
            dealMissKey(in, resultFuture)
            if (openCache()) putCache(key, CacheMissVal.getMissKeyObj)
          }
        }
      }
    }

  }

  /**
    * 从ES中查询数据
    *
    * @param inputParams List((字段值,字段名称))
    * @return
    */
  private def queryElasticsearch(inputParams: List[(Any, String)]): List[Map[String, Any]] = {
    import scala.collection.JavaConversions._

    val tmpData = mutable.ListBuffer[Map[String, Any]]()

    try {
      def getHits(scrollId: Option[String]): (String, Array[SearchHit]) = {
        scrollId match {
          case Some(id) =>
            client.scroll(id, 600000) match {
              case Success(searchRes) =>
                (searchRes.getScrollId, searchRes.getHits.getHits)
              case Failure(exception) =>
                throw exception
            }

          case None =>
            val query = QueryBuilders.boolQuery()
            inputParams.foreach {
              d =>
                query.must(QueryBuilders.matchQuery(sideInfo.equalFieldRawMap.getOrElse(d._2, d._2), d._1))
            }
            val source = new SearchSourceBuilder()
              .query(query)
              .size(100)

            val req = new SearchRequest(Array(dataSource.index), source)
              .types(dataSource.esType).scroll(new TimeValue(600000))
            client.search(req) match {
              case Success(searchRes) =>
                (searchRes.getScrollId, searchRes.getHits.getHits)
              case Failure(exception) =>
                throw exception
            }
        }
      }

      var hitsTuple = getHits(None)
      var next = true
      while (next) {
        if (hitsTuple._2.nonEmpty) {
          hitsTuple._2.foreach {
            hit =>
              tmpData.append(hit.getSourceAsMap.toMap)
          }
          hitsTuple = getHits(Option(hitsTuple._1))
        } else {
          next = false
        }
      }
    }

    catch {
      case ex: Throwable =>
        logger.error("query elasticsearch error", ex)
    }

    tmpData.toList
  }


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

  override def close(): Unit = {
    if (client != null)
      client.free()
    super.close()
  }

  def buildKey(fieldValues: List[(Any, String)]): String = {
    val output = new StringBuilder()
    fieldValues.foreach {
      f =>
        if (output.nonEmpty)
          output.append("_").append(f._1)
        else
          output.append(f._1)
    }
    output.toString()
  }
}
