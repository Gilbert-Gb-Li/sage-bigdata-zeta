package com.haima.sage.bigdata.etl.lexer

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common._
import com.haima.sage.bigdata.etl.common.exception.CollectorException
import com.haima.sage.bigdata.etl.common.model.Event

/**
  * Created by zhhuiyan on 15/3/31.
  */
abstract class EventLexer extends DefaultLexer[Event] {
  val lexer: base.Lexer[String, RichMap]

  @throws(classOf[CollectorException])
  def parse(event: Event): RichMap = event.content match {
    case null =>
      RichMap()
    case content =>
      lexer.parse(content)

  }
}
