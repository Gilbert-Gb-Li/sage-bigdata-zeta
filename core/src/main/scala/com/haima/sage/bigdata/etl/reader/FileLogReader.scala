package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{FileType, FileWrapper, ReadPosition}

abstract class FileLogReader[T](uri: String, codec: Option[Codec], contentType: Option[FileType] = None, wrapper: FileWrapper[_], override val position: ReadPosition) extends LogReader[T] with Position {

  override def path: String = uri

  def skipLine(length: Long)

  //def lineSeparatorLength: Int = wrapper.lineSeparator.length
}