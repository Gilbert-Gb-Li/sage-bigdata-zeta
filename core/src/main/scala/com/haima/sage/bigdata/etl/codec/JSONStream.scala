package com.haima.sage.bigdata.etl.codec

import java.io.IOException

import com.haima.sage.bigdata.etl.common.model.{Event, Stream}

import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object JSONStream {
  def apply(codec: JsonCodec, reader: Stream[Event]) = new JSONStream(codec, reader)

}

class JSONStream(codec: JsonCodec, reader: Stream[Event]) extends Stream[Event](codec.filter) {

  private var event: List[Char] = Nil
  private var header: Option[Map[String, Any]] = None
  private var item: Event = _


  //-1 not_ready,0 ready,1,done
  @tailrec
  private def nextChar(): Char = {

    event match {

      case head :: tails =>
        event = tails
        head
      case Nil =>
        if (reader.hasNext) {
          val e = reader.next()

          header = e.header
          event = e.content.toCharArray.toList

          nextChar()
        } else {

          // 读取完毕，设置完成状态
          state = State.done
          (-1).toChar
        }

    }
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
        makeData()
        hasNext
    }
  }

  def next(): Event = {
    init()
    item
  }

  @throws(classOf[IOException])
  override def close() {
    super.close()
    reader.close()
  }

  def makeData() {
    val builder: StringBuilder = new StringBuilder
    // “{” 计数
    var openPointer: Int = 0
    // “}” 计数
    var closePointer: Int = 0
    // (双引号标示，单引号标示) true表示开始，false表示结束
    val quotes = Array(false, false)


    var c: Char = 0

    try {
      while (! {
        c = nextChar()
        c
      }.equals((-1).toChar)) {
        // c = nextChar()
        builder.append(c)
        if (c == '\\') {
          c = nextChar()
          if (c != -1) {
            builder.append(c)
          } else {
            state = State.fail
            return
          }
        }
        c match {
          case '\"' =>
            if (!quotes(1))
              quotes(0) = !quotes(0)
          case '\'' =>
            if (!quotes(0))
              quotes(1) = !quotes(1)
          case '}' =>
            if (!quotes(0) && !quotes(1)) { // “}”不在引号中方才计数
              closePointer += 1
              //logger.debug(s"openPointer = $openPointer ,closePointer = $closePointer")
            }
            if (openPointer == closePointer) {
              // “{”和“}”数量相等，一条完整的JSON数据结束
              ready()

              item = Event(content = builder.toString())

              return
            }
          case '{' =>
            if (!quotes(0) && !quotes(1)) {
              // “{”不在引号中方才计数
              openPointer += 1
              //logger.debug(s"openPointer = $openPointer ,closePointer = $closePointer")
            }
          case _ =>
        }
      }
    }
    catch {
      case e: IOException =>
        logger.error("MK A JSON LINE ERROR:{}", e)
        state = State.fail
        throw e
    }

  }

  override def toString(): String = "JSONStream"
}

