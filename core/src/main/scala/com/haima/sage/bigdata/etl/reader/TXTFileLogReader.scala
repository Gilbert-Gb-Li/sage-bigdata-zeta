package com.haima.sage.bigdata.etl.reader

import java.io.IOException

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.exception.LogReaderInitException
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.stream.ReaderToStream._

class TXTFileLogReader(path: String, codec: Option[Codec], contentType: Option[TxtFileType] = None, wrapper: FileWrapper[_], position: ReadPosition) extends
  FileLogReader[Event](path, codec, None, wrapper, position) {

  override lazy val stream: Stream[Event] = wrap(codec, wrapper)

  override lazy val iterator: Iterator[Event] = new Iterator[Event] {

    override def next(): Event = {
      val event = stream.next()
      if (event != null) {
        event.header match {
          case Some(header) =>
            /*TODO 对于wrap的数据 要计数行数和忽略的行长度*/
            position.positionIncrement(header.getOrElse("ignoreLength", 0).toString.toInt
              + event.content.length)
          case _ =>
            position.positionIncrement(event.content.length)
        }
        position.recordIncrement()


      }
      event
    }

    override def hasNext: Boolean = {
      val has = stream.hasNext
      if (!has) {
        position.finished = true
      }
      has

    }
  }

  def skip(skip: Long): Long = try
    wrapper.stream.skip(skip)
  catch {
    case e: IOException =>
      logger.error("lineFileLogReader skip error:{}", e)
      -1

  }

  override def close(): Unit = {
    wrapper.close()
    super.close()

  }

  override def skipLine(length: Long): Unit = {
    val (records: Long, pos: Long) = wrapper.skipLine(length.toInt)
    logger.info(s"$path skip line :$length")
    position.setPosition(pos)
    position.setRecords(records)
    if (records < length - 1) {
      close()
      throw new LogReaderInitException(s"$path has process finished,don't need to  read again. records:${position.records}")
    }
  }
}