package com.haima.sage.bigdata.etl.codec

import java.io.IOException

import com.haima.sage.bigdata.etl.common.model.{Event, Stream}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object MatchStream {
  def apply(codec: MatchCodec, reader: Stream[Event]) = new MatchStream(codec, reader)
}

class MatchStream(codec: MatchCodec, reader: Stream[Event]) extends Stream[Event](codec.filter) {


  private val regex = codec.pattern.r
  private var item: Event = _


  def makeData(): Unit = {
    try {
      if (reader.hasNext) {
        item = reader.next()
        regex.findFirstIn(item.content) match {
          case Some(_) =>
            ready()
          case None =>
            init()
        }
      } else {
        finished()
      }
    }
    catch {
      case e: Exception =>
        logger.error("MK A LINE ERROR:{}", e)
        state = State.fail
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

  @throws(classOf[IOException])
  override def close() {
    reader.close()
    super.close()
  }

  override def next(): Event = {
    init()
    item
  }
}

