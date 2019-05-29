package com.haima.sage.bigdata.etl.common.exception

/**
 * Author:zhhuiyan
 *
 * DateTime:2014/7/29 13:19.
 */
class CollectorException(message: String,cause: Throwable) extends Exception(message: String,cause: Throwable) {
  def this(message: String) {
    this(message,null)
   }
}
