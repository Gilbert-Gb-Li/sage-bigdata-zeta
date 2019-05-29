package com.haima.sage.bigdata.etl.stream

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, TimeoutException}

import com.haima.sage.bigdata.etl.codec.Filter
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.model.Stream

/**
  * Created by zhhuiyan on 16/6/22.
  */
class QueueStream[T](filter: Option[Filter], timeout: Long) extends Stream[T](filter) {

  final val CACHE_SIZE: Long = try {
    CONF.getLong(PROCESS_CACHE_SIZE) match {
      case 0 =>
        PROCESS_CACHE_SIZE_DEFAULT
      case a =>
        a
    }
  } catch {
    case e: Exception =>
      logger.warn(s"if not used in flink,please check you env config for $PROCESS_CACHE_SIZE ,with cause:${e.getCause}!")
      1000
  }
  //*1000
  final val queue = new LinkedBlockingQueue[T](CACHE_SIZE.toInt + 1)

  final def hasNext: Boolean = {
    state match {
      case State.done | State.fail =>
        if (queue.size() > 0) {
          true
        } else {
          false
        }

      case State.init =>
        if (queue.size() <= 0) {
          TimeUnit.MILLISECONDS.sleep(timeout)
        }
        if (queue.size() > 0) {
          ready()
        } else {
          //          logger.debug("queue empty")
          throw new TimeoutException(s"waiting $timeout ms , no data received !")
        }
        true
      case State.ready =>
        true
    }
  }


  override def next(): T = {
    init()
    queue.poll(timeout, TimeUnit.MILLISECONDS)
  }
}
