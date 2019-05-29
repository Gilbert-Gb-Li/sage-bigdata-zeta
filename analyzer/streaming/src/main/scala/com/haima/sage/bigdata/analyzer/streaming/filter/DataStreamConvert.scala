package com.haima.sage.bigdata.analyzer.streaming.filter

import com.haima.sage.bigdata.analyzer.filter._
import com.haima.sage.bigdata.etl.common.model.{AnalyzerModel, RichMap}
import com.haima.sage.bigdata.etl.common.model.filter._
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.streaming.api.scala.DataStream


trait DataStreamConvert {
  private implicit lazy val typeInfo: GenericTypeInfo[RichMap] = new GenericTypeInfo[RichMap](classOf[RichMap])

  def engine(): AnalyzerModel.AnalyzerModel = AnalyzerModel.STREAMING

  implicit def to(stream: DataStream[RichMap]) = new {
    def map(fun: RichMap => RichMap): DataStream[RichMap] = {
      stream.map(fun)
    }

    def flatMap(fun: RichMap => List[RichMap]): DataStream[RichMap] = {
      stream.flatMap(fun)
    }

    def filter(fun: (RichMap) => Boolean): DataStream[RichMap] = stream.filter(fun)
  }
}

case class StreamAnalyzerProcessor(override val filter: ReAnalyzer) extends AnalyzerProcessor[DataStream[RichMap]] with DataStreamConvert {
  override def engine(): AnalyzerModel.AnalyzerModel = AnalyzerModel.STREAMING
}

case class StreamParserProcessor(override val filter: AnalyzerParser) extends ParserProcessor[DataStream[RichMap]] with DataStreamConvert

case class StreamScriptFilterProcessor(override val filter: AnalyzerScriptFilter) extends ScriptFilterProcessor[DataStream[RichMap]] with DataStreamConvert

case class StreamRedirectProcessor(override val filter: AnalyzerRedirect) extends RedirectProcessor[DataStream[RichMap]] with DataStreamConvert

case class StreamEndWithProcessor(override val filter: AnalyzerEndWith) extends EndWithProcessor[DataStream[RichMap]] with DataStreamConvert

case class StreamStartWithProcessor(override val filter: AnalyzerStartWith) extends StartWithProcessor[DataStream[RichMap]] with DataStreamConvert

case class StreamMatchProcessor(override val filter: AnalyzerMatch) extends MatchProcessor[DataStream[RichMap]] with DataStreamConvert

case class StreamContainProcessor(override val filter: AnalyzerContain) extends ContainProcessor[DataStream[RichMap]] with DataStreamConvert

