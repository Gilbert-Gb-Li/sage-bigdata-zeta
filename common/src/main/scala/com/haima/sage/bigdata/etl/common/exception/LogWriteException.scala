package com.haima.sage.bigdata.etl.common.exception

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/30 9:50.
  */
class LogWriteException(message: String, cause: Throwable) extends CollectorException(message: String, cause: Throwable) {
  def this(message: String) {
    this(message, null)
  }

}

