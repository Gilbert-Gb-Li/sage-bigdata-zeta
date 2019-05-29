package com.haima.sage.bigdata.etl.codec

import java.io.IOException

import com.haima.sage.bigdata.etl.common.exception.LogReaderInitException
import com.haima.sage.bigdata.etl.common.model.{Event, Stream}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
/**
  * Created by zhhuiyan on 2014/11/5.
  */
object MultiStream {
  def apply(codec: MultiCodec, reader: Stream[Event]) = new MultiStream(codec, reader)
}

class MultiStream(codec: MultiCodec, reader: Stream[Event]) extends Stream[Event](codec.filter) {
  private var header: Option[Map[String, Any]] = None
  private var item: Event = _

  //-1 not_ready,0 ready,1,done
  private val regex = codec.inStart match {
    case Some(true) =>
      s"^${codec.pattern}".r
    case Some(false) =>
      s"${codec.pattern}$$".r

    case None =>
      throw new LogReaderInitException("unknown match types")
  }
  private var cache: String = ""
  // 一条日志
  private var line: String = ""
  // 使用 开头匹配时， 匹配成功的行文本缓存
  private var line_cache: String = ""
  // 一条完整的 Event 忽略的字节数
  private var realLength = 0
  // 最大匹配行数
  private val MAX = codec.maxLineNum match {
    case Some(a: String) =>
      Try(a.toLong) match {
        case Success(_) => a.toLong
        case Failure(_) => 1000l
      }
    case _ => 1000l
  }
  // 每条日志 匹配行计数
  private var i = 0
  // 是否匹配成标示
  private var mached = false

  def addCache(): Unit = {
    //line += s"\n$cache"
    line += s"$cache"
  }

  // 获取下一行文本
  private def getEvent: Unit = {
    if (reader.hasNext) {
      val event = reader.next()
      header = event.header
      cache = event.content
      if (i == MAX)
        //throw new LogReaderException(s"Stack max then $MAX,  StackOverflowError, maybe you codec config error")
        logger.warn(s"With the number of rows is greater than the maximum number of rows with $MAX")
      i += 1
      // 记录 event 的 real length
      realLength = realLength + cache.length
      // 设置文件标示
      init()
    } else {
      // 设置文件标示
      finished()
    }
  }

  // 获取一条日志
  private def lineProcess(): String = {

    // 一条日志标示
    var lineFinish = false
    // 一条日志
    var line1: String = ""
    // 获取下一行文本
    getEvent

    // 一条日志结束或者文件结束，循环退出，返回line1
    while (!lineFinish && state != State.done) {
      (regex.findFirstIn(cache), codec.inStart) match {
        case (Some(_), Some(true)) =>
          mached = true
          if (line1 == "") {
            if (line_cache != "") {
              line1 = line_cache
              // 把匹配开头的一行文本缓存起来
              line_cache = cache
              lineFinish = true
            } else {
              // 把匹配开头的一行文本缓存起来
              line_cache = cache
              getEvent
            }
          } else {
            // 把匹配开头的一行文本缓存起来
            line_cache = cache
            cache = ""
            // 置一条日志结束状态
            lineFinish = true
          }
        case (_, Some(true)) =>
          //line1 += line_cache + "\t" + cache
          line1 += line_cache + cache
          // 置空 行缓存
          line_cache = ""
          getEvent


        case (Some(_), Some(false)) =>
          mached = true

          //行结束
          /*if (line1 != "")
            line1 += "\t"*/
          line1 += cache
          cache = ""
          // 置一条日志结束状态
          lineFinish = true

        case (_, Some(false)) =>
          /* if (line1 != "")
             line1 += "\t"*/
          line1 += cache
          getEvent
        case _ =>

      }
    }
    line1
  }


  @tailrec
  final def hasNext: Boolean = {
    state match {
      case State.ready =>
        true
      case State.done =>
        false
      case State.fail =>
        false
      case _ =>
        makeData
        hasNext
    }
  }

  def makeData: Boolean = {
    try {
      item = null
      //      line = ""
      mached = false
      i = 0
      line = lineProcess()
      if (line != null && line.trim != "") {
        ready()

        item = Event(Option(header.getOrElse(Map()) + ("ignoreLength" -> (realLength - line.length))), line)
        realLength = 0
      }
    }
    catch {
      case e: Exception =>
        logger.error("MK A LINE ERROR:{}", e)
        item = null
        state = State.fail
        throw e
    }
    item != null
  }

  @throws(classOf[IOException])
  override def close() {
    super.close()
    reader.close()
  }

  override def next(): Event = {
    init()
    item
  }
}

