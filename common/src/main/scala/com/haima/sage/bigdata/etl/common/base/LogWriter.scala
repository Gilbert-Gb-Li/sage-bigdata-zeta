package com.haima.sage.bigdata.etl.common.base

import com.haima.sage.bigdata.etl.common.exception.LogWriteException
import com.haima.sage.bigdata.etl.common.model.{RichMap, Writer}
import com.haima.sage.bigdata.etl.utils.Logger

trait LogWriter[CONFIG <: Writer] extends Logger {
  @throws(classOf[LogWriteException])
  def write(batch: Long,t: List[RichMap]): Unit


  def redo(): Unit

  def tail(num: Int): Unit

  def flush(): Unit

  def close(): Unit
}
