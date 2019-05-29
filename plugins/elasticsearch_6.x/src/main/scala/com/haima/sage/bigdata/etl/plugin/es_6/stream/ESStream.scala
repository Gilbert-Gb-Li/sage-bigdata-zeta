package com.haima.sage.bigdata.etl.plugin.es_6.stream

import java.io.IOException
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{ES6Source, RichMap, Stream}
import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object ESStream {
  def apply(conf: ES6Source, value: Any, step: Long): ESStream = {
    new ESStream(conf: ES6Source, value, step)
  }


  class ESStream(conf: ES6Source, value: Any, step: Long) extends Stream[RichMap](None) {
    private final val format_utc = new ThreadLocal[DateFormat]() {
      protected override def initialValue(): DateFormat = {
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
      }
    }


    private val client = ElasticClient(conf.cluster, conf.hostPorts)

    private def check() = client.indicesExists(conf.index).isSuccess

    private var exists: Boolean = check()

    var current: Any = value

    val sizes: Int = conf.size

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
                case Failure(_) => d1.toLong
                case Success(v) => v
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


    private var scrollId: String = _

    private def query(): Array[SearchHit] = {
      if (scrollId == null) {
        if (!exists) {
          logger.warn(s" index[${conf.index}]/type[${conf.esType}] is not exists")
          TimeUnit.MILLISECONDS.sleep(conf.timeout)
          exists = check()
          if (!exists) {
            throw new TimeoutException(s"elasticsearch:after[${conf.timeout} ms]  timeout,cause:index[${conf.index}]/type[${conf.esType}] is not exists")
          }
        }

        val queries = conf.queryDSL match {
          case Some(queryStr) if StringUtils.isNotEmpty(queryStr.trim) =>
            QueryBuilders.boolQuery()
              .must(QueryBuilders.wrapperQuery(queryStr))
              .must(QueryBuilders.rangeQuery(conf.field)
                .from(current match {
                  case d: Date =>
                    d.getTime
                  case d => d
                }).includeLower(false)
                .includeUpper(true))
          case _ =>
            QueryBuilders.rangeQuery(conf.field)
              .from(current match {
                case d: Date =>
                  d.getTime
                case d => d
              })
              .includeLower(false)
              .includeUpper(true)
        }
        client.search(new SearchRequest(Array(conf.index), new SearchSourceBuilder()
          .sort(conf.field, SortOrder.ASC)
          .query(queries)
          .size(sizes).timeout(TimeValue.timeValueMinutes(1)) //how many hits per shard will be returned for each scroll

          .explain(true)).types(conf.esType).scroll(new TimeValue(60000))) match {
          //设置是否按照查询匹配度进行搜索
          case Success(r) =>
            r.getHits.getHits

          case Failure(e) =>
            logger.error(s"with elastic error:$e")
            throw e
        }
      } else {
        client.scroll(scrollId, 60000) match {
          case Success(r) =>

            r.getHits.getHits

          case Failure(_) => //处理scrollId失效的情况
            scrollId = null
            null
        }
      }

    }

    @tailrec
    override final def hasNext: Boolean = {
      if (state == State.init || state == State.ready) {
        if (set == null || set.isEmpty) {
          set = query().toList
          if (set == null || set.isEmpty) {
            TimeUnit.MILLISECONDS.sleep(conf.timeout)
            set = query().toList
            if (set == null || set.isEmpty) {
              throw new TimeoutException(s"after ${conf.timeout} ms elasticsearch read timeout!")
            }
          }

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

        set match {
          case Nil =>
            null
          case head :: tail =>
            import scala.collection.JavaConversions._
            var item: Map[String, Any] = head.getSourceAsMap.toMap
            item = item.get(conf.field) match {
              case Some(d) =>
                current = become(d)
                item + conf.field.->(current)
              case None =>
                item - conf.field
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


