package com.haima.sage.bigdata.etl.stream

import java.io.{InputStream, Serializable}
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.alibaba.fastjson.JSONObject
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.{PrometheusSource, Stream}
import com.haima.sage.bigdata.etl.utils.Mapper

import scala.annotation.tailrec
import scala.io.Source
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

object PrometheusStream {
  def apply(conf: PrometheusSource): PrometheusStream = {
    new PrometheusStream(conf)
  }

  case class PrometheusQueryResult(status: String, data: Data) extends Serializable

  case class Data(resultType: String, result: Option[List[JSONObject]] = None) extends Serializable

  class PrometheusStream(conf: PrometheusSource) extends Stream[RichMap](None) with Mapper {

    val timeout: Long = conf.timeout

    var count = 0
    var current: Long = getStartTime(conf.start)
    private var _info: String = ""
    private var isConnectFlag = true

    private var item: RichMap = _

    override def toString(): String = {
      conf.uri
    }

    private var set: Iterator[RichMap] = _

    private var inputStream: InputStream = _

    private var url: String = _

    /**
      * 根据时间的检查列，判断还有没有新的数据， 有的话初始化数据查询URL语句，否则不处理（多次检查后抛出异常）
      **/
    implicit def time(last: Long): Boolean = {
      //logger.debug(s"current is ${current}, max is ${last}")
      if (last <= current) {
        current = last
        false
      } else {
        val startTime: Long = getStartTime(current + conf.step) //转成unix值
        current = startTime + conf.rangeStep
        if (last < current)
          current = last
        url = s"http://${conf.host}:${conf.port}/api/v1/query_range?query=${conf.expression}&start=$startTime&end=$current&step=${conf.step}s"
        //logger.debug(s"url=${url}")
        true
      }
    }


    def max(): Long = System.currentTimeMillis() / 1000

    def hasMore: Boolean = time(max())


    @tailrec
    final def hasNext: Boolean = {
      state match {
        case State.ready =>
          true
        case State.done =>
          if (set == null || set.isEmpty) {
            false
          } else {
            makeData()
            hasNext
          }
        case State.fail =>
          false
        case _ =>
          if (set == null || set.isEmpty)
            Try(
              set = makeResultData(get())
            ) match {
              case Success(_) => isConnectFlag = true
              case Failure(e) =>
                _info = s"get Restful Connect error:${e.getMessage}"
                isConnectFlag = false
                logger.error(_info)
                throw e
            }
          makeData()
          hasNext

      }
    }

    def makeData() {
      if (set != null && set.hasNext) {
        item = set.next()
        if (state != State.done) {
          ready()
        }
      }
    }

    def get(connectTimeout: Int = 1000, readTimeout: Int = 1000, requestMethod: String = "GET"): PrometheusQueryResult = {
      if (isConnectFlag) {
        if (!hasMore) {
          logger.debug(s"has no more data, thread will sleep for 1000 milliseconds")
          TimeUnit.MILLISECONDS.sleep(1000)
          if (!hasMore) {
            throw new TimeoutException(s"after 1000 ms prometheus read timeout!")
          }
        }
      }

      import java.net.{HttpURLConnection, URL}
      val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestMethod(requestMethod)
      inputStream = connection.getInputStream
      val content = Source.fromInputStream(inputStream).mkString
      if (inputStream != null) inputStream.close()
      mapper.readValue[PrometheusQueryResult](content)
    }

    implicit def toLong(data: Any): Long = data match {
      case last: Int =>
        last.toLong
      case last: Double =>
        last.toLong
    }

    /**
      * 构建结果集
      *
      * @param prometheusQueryResult 查询结果
      * @return Iterator Map[String,Any]迭代器集合（重新组织）
      */
    def makeResultData(prometheusQueryResult: PrometheusQueryResult): Iterator[RichMap] = {

      val result = prometheusQueryResult.data.result match {
        case Some(r) => r
        case None => Nil
      }
      result.flatMap(f = jsonObject => {
        val iterator = jsonObject.values().iterator()
        val metric = iterator.next().asInstanceOf[Map[String, Any]]
        val values = iterator.next().asInstanceOf[List[List[Any]]]
        values.map(value => {
          var dataMap = metric
          dataMap += ("timestamp" -> value.head.toLong)
          dataMap += ("value" -> value(1).asInstanceOf[String])
          RichMap(dataMap)
        })

      }).iterator
    }

    /**
      * 处理开始时间，转成unix秒值
      *
      * @param start 开始时间北京时间秒值
      * @return
      */
    def getStartTime(start: Long): Long = start

    final override def next(): RichMap= {
      init()
      item
    }
  }

}


