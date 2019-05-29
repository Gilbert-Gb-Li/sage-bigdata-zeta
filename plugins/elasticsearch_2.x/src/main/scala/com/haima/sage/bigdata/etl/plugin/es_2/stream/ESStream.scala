package com.haima.sage.bigdata.etl.plugin.es_2.stream

import java.io.IOException
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date
import java.util.concurrent.{TimeUnit, TimeoutException}
import javax.activation.UnsupportedDataTypeException

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.Stream
import com.haima.sage.bigdata.etl.plugin.es_2.client.ElasticClient
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder

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

    private val check = client.get().admin().indices()
      .prepareTypesExists(indices)
      .setTypes(esType)
    private var exists: Boolean = check.execute().actionGet().isExists

    var current: Any = value
    current = min()
    var from = 0
    val sizes = 10000
    var maxed = 0l

    def lower(d: Any): Any = {
      d match {
        case d1: Double =>
          d1.toLong - 1
        case date: Long =>
          date - 1
        case a: Date =>
          new Date(a.getTime - 1)
        case d1 =>
          throw new UnsupportedDataTypeException(s"data:$d1,${d1.getClass}")

      }
    }

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


    def hasMore: Boolean = {

      current match {
        case date: Date =>
          val d = date.getTime
          val d2 = max().asInstanceOf[Date].getTime
          d < d2
        case v: Long =>
          val d2 = max().asInstanceOf[Long]
          v < d2
        case v =>
          throw new UnsupportedDataTypeException(s"data:$v")
      }
    }

    def max(): Any = {
      val builder: SearchRequestBuilder = client.get().prepareSearch(indices)
      builder.setTypes(esType)
      builder.addSort(field, SortOrder.DESC).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      val response: SearchResponse = builder.setSize(1).setExplain(true).execute.actionGet
      val hits = response.getHits
      if (hits.getTotalHits <= 0) {
        current
      } else {
        val data = hits.getHits()(0).getSource
        data.get(field) match {
          case null =>
            current
          case d =>
            become(d)
        }

      }
    }

    def min(): Any = {
      val builder: SearchRequestBuilder = client.get().prepareSearch(indices)

      builder.setTypes(esType)
      builder.addSort(field, SortOrder.ASC).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.rangeQuery(field)
        .from(current match {
          case d: Date =>
            d.getTime
          case d => d
        })
        .includeLower(true))
      val response: SearchResponse = builder.setSize(1).setExplain(true).execute.actionGet
      val hits = response.getHits
      if (hits.getTotalHits <= 0) {
        current
      } else {
        val data = hits.getHits()(0).getSource
        data.get(field) match {
          case null =>
            current
          case d =>
            lower(become(d))
        }

      }
    }


    def last: Any = {
      current match {
        case date: Date =>
          val d = new Date().getTime
          val d2 = date.getTime + step * 1000
          if (d < d2) {
            new Date()
          } else {
            new Date(d2)
          }
        case v: Long =>
          v + step
        case v: Int =>
          v + step.toLong
        case v =>
          throw new UnsupportedDataTypeException(s"data:$v,${v.getClass}")
      }
    }


    private var set: List[SearchHit] = Nil

    @tailrec
    private def query(): List[SearchHit] = {

      if (!exists) {
        logger.warn(s"your set index[$indices]/type[$esType] is not exists")
        TimeUnit.MILLISECONDS.sleep(timeout)
        exists = check.execute().actionGet().isExists
        if (!exists) {
          throw new TimeoutException(s"elasticsearch:after[$timeout ms]  read timeout!")
        }
      }

      val builder: SearchRequestBuilder = client.get().prepareSearch(indices)
      builder.setTypes(esType).addSort(field, SortOrder.ASC)
        .setQuery(QueryBuilders.rangeQuery(field)
          .from(current match {
            case d: Date =>
              d.getTime
            case d => d
          }).to(last match {
          case d: Date =>
            d.getTime
          case d => d
        })
          .includeLower(false)
          .includeUpper(true))
        //                          .setFrom(from)
        .setSize(sizes) //how many hits per shard will be returned for each scroll
        .setScroll(TimeValue.timeValueMinutes(1))
        .setExplain(true)

      var scrollResp: SearchResponse = builder.execute.actionGet

      import org.elasticsearch.common.unit.TimeValue

      import scala.collection.mutable.ArrayBuffer
      var list = ArrayBuffer[SearchHit]()
      //Scroll until no hits are returned

      while (scrollResp.getHits.getHits.length >= sizes) {
        scrollResp.getHits.getHits.foreach(list.append(_))
        if (scrollResp.getHits.getHits.length == sizes)
          scrollResp = client.get().prepareSearchScroll(scrollResp.getScrollId)
            .setScroll(TimeValue.timeValueMinutes(1)).execute.actionGet
      }
      if (scrollResp.getHits.getHits.length >= 0) {
        scrollResp.getHits.getHits.foreach(list.append(_))
      }

      maxed = list.length
      logger.debug(s"indices:$indices time: ${format_utc.get().format(current)}->${format_utc.get().format(last)} from:$from,sizes:$sizes,take: $maxed")
      if (maxed <= 0) {
        from = 0
        if (!hasMore) {
          TimeUnit.MILLISECONDS.sleep(timeout)
          if (!hasMore) {
            throw new TimeoutException(s"after $timeout ms es  read timeout!")
          }
        }
        current = last
        current = min()
        query()
      } else {

        if (from + sizes <= maxed) {
          from = from + sizes
        } else {
          from = 0
          current = last
        }
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


