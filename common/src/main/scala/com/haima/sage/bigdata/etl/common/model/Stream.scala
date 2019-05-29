package com.haima.sage.bigdata.etl.common.model

import java.io.{Closeable, _}

import com.haima.sage.bigdata.etl.codec.Filter
import com.haima.sage.bigdata.etl.utils.Logger


/**
  * Created by zhhuiyan on 2014/11/5.
  */
abstract class Stream[T](filter: Option[Filter]) extends Iterator[T] with Closeable with Serializable with Logger {




  object State extends Enumeration {
    type State = Value

    final val init, ready, done, fail = Value
  }

  @volatile
  protected var state: State.Value = State.init

  def isRunning: Boolean = {
    state != State.done && state != State.fail
  }

  def init(): Unit = {
    state match {
      case State.done | State.fail =>
      case _ =>
        state = State.init
    }
  }


  def ready(): Unit = {
    state = State.ready
  }

  def finished(): Unit = {
    state = State.done
  }

  @throws(classOf[IOException])
  override def close() {
    finished()
  }

  def remove() {
    if (this.hasNext) {
      this.next()
    }
  }
}
