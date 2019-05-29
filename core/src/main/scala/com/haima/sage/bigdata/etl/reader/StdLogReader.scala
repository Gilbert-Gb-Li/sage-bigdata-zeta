package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{DataSource, Event, Stream}

import scala.annotation.tailrec
import scala.io.StdIn


class StdLogReader(conf: DataSource) extends LogReader[Event] {


  override def skip(skip: Long): Long = 0

  override val stream: Stream[Event] = StdStream()

  override def path: String = {
    conf.name
  }

  case class StdStream() extends Stream[Event](None) {
    private var item: Event = null

    //-1 not_ready,0 ready,1,done


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

      println()
      println("please input[ bye will closed]:")
      val line = StdIn.readLine()
      if ("bye".equals(line)) {
        println(" bye bye! ")

        finished()
      } else {
        ready()
        item = Event(None, line)
      }


    }

    override def close(): Unit = {
      super.close()
    }

    override def next(): Event = {
      init()
      item
    }
  }

}

