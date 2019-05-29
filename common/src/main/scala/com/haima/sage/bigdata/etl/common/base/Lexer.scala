package com.haima.sage.bigdata.etl.common.base

import com.haima.sage.bigdata.etl.common.exception.{CollectorException, LogParserException}
import com.haima.sage.bigdata.etl.common.model.{Parser => MParser}
import com.haima.sage.bigdata.etl.utils.Logger

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/23 15:23.
  */
trait Lexer[F, T] extends Serializable with Logger {

  @throws(classOf[LogParserException])
  def parse(from: F):  T
}



