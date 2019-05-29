package com.haima.sage.bigdata.etl.plugin.es_5.stream

import java.io.IOException
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.Stream
import com.haima.sage.bigdata.etl.plugin.es_5.client.ElasticClient
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.common.unit.TimeValue

import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object ESStream {
  def apply(cluster: String, hostPorts: Array[(String, Int)], indices: String, esType: String, field: String, value: Any, step: Long, timeout: Long): ESStream = {
    new ESStream(cluster, hostPorts, indices, esType, field, value, step, timeout)
  }


  class ESStream(cluster: String, hostPorts: Array[(String, Int)], indices: String, esType: String, field: String, value: Any, step: Long, timeout: Long) extends Stream[RichMap](None) {
    private final val format_utc = new ThreadLocal[DateFormat]() {
      protected override def initialValue(): DateFormat = {
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
      }
    }


    private val client = ElasticClient(cluster, hostPorts)

    private val check = client.get().admin().indices().prepareTypesExists(indices).setTypes(esType)
    private var exists: Boolean = check.execute().actionGet().isExists

    var current:Any=value

    val sizes = 10000

    def become(d: Any): Any = {
      current match {
        case _: Date =>
          d match {
            case d1: Double =>
              new Date(d1.toLong)
            case date: Long =>
              new Date(date)
            case _ =>
              new Date(format_utc.get().parse(d.toString).getTime + 8 * 60 * 60 * 1000)
          }

        case _: Long =>
          d match {
            case d1: Double =>
              d1.toLong
            case _: Long =>
              d
            case d1: String =>
              Try(format_utc.get().parse(d1.toString).getTime + 8 * 60 * 60 * 1000) match {
                case Failure(_)=> d1.toLong
                case Success(v)=>v
              }
            case _ =>
              d.toString.toLong
          }
        case c: String if c.matches("[\\d]+\\.\\d*") =>
          d.toString.toDouble
        case _: String =>
          new Date(format_utc.get().parse(d.toString).getTime + 8 * 60 * 60 * 1000)
        case _ =>

      }
    }

    private var set: List[SearchHit] = Nil

    private var builder: SearchRequestBuilder = _

    private var scrollResp: SearchResponse = _

    private var scrollRespFinished: Boolean = true

    private def query(): List[SearchHit] = {
      var maxed = 0l
      if (scrollRespFinished) {
        if (!exists) {
          logger.warn(s"your set index[$indices]/type[$esType] is not exists")
          TimeUnit.MILLISECONDS.sleep(timeout)
          exists = check.execute().actionGet().isExists
          if (!exists) {
            throw new TimeoutException(s"elasticsearch:after[$timeout ms]  read timeout!")
          }
        }

        builder = client.get().prepareSearch(indices)
          .setTypes(esType)
          .addSort(field, SortOrder.ASC)
          .setQuery(QueryBuilders.rangeQuery(field)
            .from(current match {
              case d: Date =>
                d.getTime
              case d => d
            })
            .includeLower(false)
            .includeUpper(true))
          .setSize(sizes) //how many hits per shard will be returned for each scroll
          .setScroll(new TimeValue(60000))
          .setExplain(true) //设置是否按照查询匹配度进行搜索
        scrollResp = builder.execute.actionGet
      }

      val list = ArrayBuffer[SearchHit]()

      val hit = scrollResp.getHits.getHits
      //对于一个时间间隔数据特别大的情况，性能特别差，耗内存
      if (hit.length == sizes) {
        hit.foreach(list.append(_))
        val scrollId = scrollResp.getScrollId
        Try(
          scrollResp = client.get().prepareSearchScroll(scrollId).setScroll(new TimeValue(60000)).execute.actionGet
        ) match {
          case Success(_) =>
            scrollRespFinished = false
          case Failure(_) => //处理scrollId失效的情况
            scrollRespFinished = true
        }
      } else {
        scrollRespFinished = true
        hit.foreach(list.append(_))
      }

      maxed = list.length.toLong
      if (maxed == 0l) {
        if (scrollRespFinished) {
          TimeUnit.MILLISECONDS.sleep(timeout)
          if (scrollRespFinished) {
            throw new TimeoutException(s"after $timeout ms es  read timeout!")
          }
        }
        query()
      } else {
        current = become(list.last.getSource.get(field))
        list.toList
      }
    }

    @tailrec
    override final def hasNext: Boolean = {
      if (state == State.init || state == State.ready) {
        if (set == null || set.isEmpty) {
          set = query()
          hasNext
        } else {
          true
        }
      } else {
        false
      }
    }


    final override def next(): RichMap = {
      try {
        import scala.collection.JavaConversions._
        set match {
          case Nil =>
            null
          case head :: tail =>
            var item: Map[String, Any] = head.getSource.toMap
            item = item.get(field) match {
              case Some(d) =>
                item + field.->(become(d))
              case None =>
                item - field
            }
            set = tail
            item
        }
      } catch {
        case e: Exception =>
          logger.error(s"es get row date error:$e")
          state = State.fail
          null
      }
    }


    @throws(classOf[IOException])
    override def close() {
      super.close()
      client.free()
    }

  }

}


