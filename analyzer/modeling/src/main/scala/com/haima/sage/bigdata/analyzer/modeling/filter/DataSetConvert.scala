package com.haima.sage.bigdata.analyzer.modeling.filter

import com.haima.sage.bigdata.analyzer.filter._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.AnalyzerModel
import com.haima.sage.bigdata.etl.common.model.filter._
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.scala.DataSet

trait DataSetConvert extends Serializable {
  def engine(): AnalyzerModel.AnalyzerModel = AnalyzerModel.MODELING

  private implicit lazy val typeInfo: GenericTypeInfo[RichMap] = new GenericTypeInfo[RichMap](classOf[RichMap])

  implicit def to(stream: DataSet[RichMap]) = new {
    def map(fun: RichMap => RichMap): DataSet[RichMap] = {
      stream.map(fun)
    }
    def flatMap(fun: RichMap => List[RichMap]): DataSet[RichMap] = {
      stream.flatMap(fun)
    }

    def filter(fun: (RichMap) => Boolean): DataSet[RichMap] = stream.filter(fun)
  }
}

case class ModelingParserProcessor(override val filter: AnalyzerParser) extends ParserProcessor[DataSet[RichMap]] with DataSetConvert

case class ModelingScriptFilterProcessor(override val filter: AnalyzerScriptFilter) extends ScriptFilterProcessor[DataSet[RichMap]] with DataSetConvert

case class ModelingRedirectProcessor(override val filter: AnalyzerRedirect) extends RedirectProcessor[DataSet[RichMap]] with DataSetConvert

case class ModelingEndWithProcessor(override val filter: AnalyzerEndWith) extends EndWithProcessor[DataSet[RichMap]] with DataSetConvert

case class ModelingStartWithProcessor(override val filter: AnalyzerStartWith) extends StartWithProcessor[DataSet[RichMap]] with DataSetConvert

case class ModelingMatchProcessor(override val filter: AnalyzerMatch) extends MatchProcessor[DataSet[RichMap]] with DataSetConvert

case class ModelingContainProcessor(override val filter: AnalyzerContain) extends ContainProcessor[DataSet[RichMap]] with DataSetConvert

