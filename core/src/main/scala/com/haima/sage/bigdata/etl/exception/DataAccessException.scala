package com.haima.sage.bigdata.etl.exception

/**
  * Created: 2016-05-16 16:32.
  * Author:zhhuiyan
  * Created: 2016-05-16 16:32.
  *
  *
  */
case class DataAccessException(msg: String)

case class IncorrectResultSizeDataAccessException(msg: String) extends Exception

case class EmptyResultDataAccessException(msg: String) extends Exception