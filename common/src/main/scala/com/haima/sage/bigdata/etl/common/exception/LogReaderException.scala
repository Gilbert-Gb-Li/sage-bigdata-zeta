package com.haima.sage.bigdata.etl.common.exception

/**
 * Created by zhhuiyan on 15/1/29.
 */
class LogReaderException(message: String, cause: Throwable)  extends CollectorException(message: String, cause: Throwable) {
  def this(message: String) {
    this(message, null)
  }
}
class LogReaderInitException(message: String, cause: Throwable)  extends LogReaderException(message: String, cause: Throwable) {
  def this(message: String) {
    this(message, null)
  }
}