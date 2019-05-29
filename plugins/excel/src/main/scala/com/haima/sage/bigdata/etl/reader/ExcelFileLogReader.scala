package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.exception.LogReaderInitException
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.stream.ExcelStream

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/8/1 10:01.
  */
class ExcelFileLogReader(uri: String, codec: Option[Codec], contentType: Option[ExcelFileType] = None, wrapper: FileWrapper[_], position: ReadPosition) extends
  FileLogReader[RichMap](uri, codec, contentType, wrapper, position) {
  def getHasHeader: Boolean = {
    true
  }

  def getSeparator: String = {
    ""
  }

  //  override val stream: Stream[RichMap] = ExcelStream(path,position.position)
  override lazy val stream: Stream[RichMap] = ExcelStream(wrapper, contentType)

  override lazy val iterator: Iterator[RichMap] = new Iterator[RichMap] {
    override def next(): RichMap = {
      position.positionIncrement(1)
      position.recordIncrement()
      stream.next()
    }

    override def hasNext: Boolean = {
      val has = stream.hasNext
      if (!has) {
        position.finished = true
      }
      has

    }
  }

  def skip(skip: Long): Long = {

    if (skip == 0) {
      0
    } else {
      var i: Int = 0
      while (stream.hasNext) {
        stream.next()
        i += 1
        if (i == skip) {
          return skip
        }
      }
      i
    }

  }

  override def skipLine(length: Long): Unit = {
    if (skip(length) < length - 1) {
      close()
      throw new LogReaderInitException(s"$path has process finished,don't need to  read again. records:${position.records}")
    }
  }
}
