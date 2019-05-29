package com.haima.sage.bigdata.analyzer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.{AnalyzerParser, ReParser}
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import com.haima.sage.bigdata.etl.lexer.filter.ReParserProcessor

trait ParserProcessor[T] extends RuleProcessor[T, List[T], AnalyzerParser] {

  private lazy val parser: ReParserProcessor = ReParserProcessor(ReParser(parser = filter.parser.orNull))


  implicit def to(stream: T): {def map(fun: RichMap => RichMap): T
    def flatMap(fun: RichMap => List[RichMap]): T
  }

  override def process(stream: T): List[T] = {
    filter.parser match {
      case Some(_) =>
        List[T](stream.flatMap(event => {
          parser.process(event)
        }))
      case None =>
        List(stream)
    }

  }
}

