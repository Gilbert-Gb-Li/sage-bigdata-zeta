package com.haima.sage.bigdata.etl.reader

import java.io.IOException

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.exception.LogReaderInitException
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.stream.CSVStream


class CSVFileLogReader(path: String, codec: Option[Codec] = None, contentType: Option[CSVFileType] = None, wrapper: FileWrapper[_], position: ReadPosition) extends FileLogReader[RichMap](path, codec, None, wrapper, position) {

  override val stream: CSVStream.CSVStream = CSVStream.file2Stream(wrapper, contentType match {

    case Some(ctype) if ctype.isHaveThead =>
      None
    case Some(ctype) =>
      ctype.header.map(_.split(","))
    case None =>
      None
  })

  override lazy val iterator: Iterator[RichMap] = new Iterator[RichMap] {

    override def next(): RichMap = {
      val event = stream.next()
      if (event != null) {
        val length = event.getOrElse("length", 0l).toString.toLong
        position.positionIncrement(length)
        position.recordIncrement()
      }
      event - "length"
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
    stream.skip(skip)
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
    val (records: Long, pos: Long) = {
      (0 until length.toInt).map {
        _ =>
          if (stream.hasNext) {
            val length = stream.next().getOrElse("length", 0l).toString.toLong
            (1l, length)
          } else {
            (0l, 0l)
          }
      }.reduce[(Long, Long)] {
        case (f, second) =>
          (f._1 + second._1, f._2 + second._2)
      }
    } //wrapper.skipLine(length.toInt)
    logger.info(s"$path skip line :$length")
    position.setPosition(pos)
    position.setRecords(records)
    if (records < length - 1) {
      close()
      throw new LogReaderInitException(s"$path has process finished,don't need to  read again. records:${position.records}")
    }
  }
}