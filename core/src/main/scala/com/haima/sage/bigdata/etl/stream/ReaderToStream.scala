package com.haima.sage.bigdata.etl.stream

import java.io.BufferedReader

import com.haima.sage.bigdata.etl.common.model.{Event, FileWrapper, ReaderLineReader, Stream}

import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object ReaderToStream {

  /*FileWrapper[_]*/
  implicit def file2Stream(wrapper: FileWrapper[_]): ReaderToStream = new ReaderToStream(wrapper)

  implicit def reader2Stream(reader: ReaderLineReader): ReaderToStream = new ReaderToStream(reader)

  class ReaderToStream(reader: ReaderLineReader) extends Stream[Event](None) {

    var header: Option[Map[String, Any]] = None


    def this(wrapper: FileWrapper[_]) {
      this(wrapper.stream)
      try {
        header = Option(wrapper.metadata)
      } catch {
        case e: Exception =>
          if (reader != null)
            reader.close()
          finished()
          logger.error(s"read from file has error:${e.getMessage}")
      }

    }

    var item: Event = _

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

    def makeData(): Unit = {
      try {

        if (reader != null) {
          reader.readLine match {
            case null =>
              finished()
            case line =>
              ready()
              item = Event(header, line)
          }

        } else {
          if (reader != null)
            reader.close()
          finished()
        }
      }
      catch {
        case e: Exception =>
          logger.error("MK A LINE ERROR:{}", e)
          item = null
          reader.close()
          finished()
          throw e
      }
    }


    override def next(): Event = {
      init()
      item
    }

    override def close(): Unit = {
      if (reader != null)
        reader.close()
      super.close()
    }
  }

}

