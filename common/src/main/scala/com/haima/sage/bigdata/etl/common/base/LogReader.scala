package com.haima.sage.bigdata.etl.common.base

import java.io.Closeable
import java.util.concurrent.TimeoutException

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.Stream
import com.haima.sage.bigdata.etl.utils.Logger

import scala.collection.mutable.ListBuffer

trait LogReader[T] extends Iterable[T] with Closeable with Logger {
  var running = true


  var count = 0


  def skip(skip: Long): Long

  def path: String

  val stream: Stream[T]

  def wrap(codec: Option[Codec], stream: Stream[T]): Stream[T] = {
    codec match {
      case Some(cc) =>
        val clazzName: String = Constants.CODECS.getString(cc.name)
        logger.debug(s"codec:$clazzName")
        if (clazzName != null) {
          val codecClass: Class[Stream[T]] = Class.forName(clazzName).asInstanceOf[Class[Stream[T]]]
          codecClass.getConstructor(cc.getClass, classOf[Stream[T]]).
            newInstance(cc, stream)
          /*if (codecClass.equals(classOf[LineStream])) {
            stream
          } else {

          }*/

        }
        else {
          throw new NullPointerException(s"config  reader error ,can't find your conf of codec:${cc.name}")
        }
      case None =>
        stream
    }

  }

  def take(time: Long, num: Int): (Iterable[T], Boolean) = {


    @volatile var out = false
    val now = System.currentTimeMillis()

    val data = new ListBuffer[T]
    try {
      var i = 0
      while (i < num && !out && iterator.hasNext) {
        data.+=(iterator.next())
        i += 1
        if (System.currentTimeMillis() - now >= time) {
          out = true
        }
      }
      (data.toList, out)
    } catch {
      case e: TimeoutException =>
        if (count % 1000 == 0) {
          logger.warn(s"timeout $count :$e")
        }
        count += 1
        out = true
        (data.toList, true)
      case e: java.io.FileNotFoundException =>
        if (count % 1000 == 0) {
          logger.error(s"ignore must be care:$e")
        }

        count += 1
        out = true
        (data.toList, true)
      case e: Exception =>
        e.printStackTrace()
        if (count % 1000 == 0) {
          logger.error(s"ignore must be care:$e")
        }
        count += 1
        out = true
        (data.toList, true)
      case ex: Throwable =>
        logger.error(s"log reader error th:$ex")
        throw ex
    }

  }

  override def iterator: Iterator[T] = stream

  def isClosed: Boolean =
    !running


  override def close(): Unit = {
    running = false
    if (stream.isRunning)
      stream.close()
  }
}
