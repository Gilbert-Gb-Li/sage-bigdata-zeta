package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.exception.LogReaderInitException
import com.haima.sage.bigdata.etl.common.model
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.stream.WinEvtStream

class WinEvtFileLogReader(uri: String, codec: Option[Codec], contentType: Option[WinEvtFileType] = None, wrapper: FileWrapper[_], position: ReadPosition) extends
  FileLogReader[Event](uri, codec, contentType, wrapper, position) {
  val stream: model.Stream[Event] = wrap(codec, WinEvtStream(path))

  override val iterator: Iterator[Event] = new Iterator[Event] {
    override def next(): Event = {
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
    var i: Int = 0
    while (stream.hasNext) {
      i += 1
      if (i == skip) {
        return skip
      }
    }
    i
  }

  override def skipLine(length: Long): Unit = {
    if (skip(length) < length - 1) {
      close()
      throw new LogReaderInitException(s"$path has process finished,don't need to  read again. records:${position.records}")
    }
  }
}