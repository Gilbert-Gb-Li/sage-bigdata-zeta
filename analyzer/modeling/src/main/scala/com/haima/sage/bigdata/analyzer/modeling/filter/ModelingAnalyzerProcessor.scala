package com.haima.sage.bigdata.analyzer.modeling.filter

import com.haima.sage.bigdata.analyzer.filter.AnalyzerProcessor
import com.haima.sage.bigdata.etl.common.model.filter.ReAnalyzer
import com.haima.sage.bigdata.etl.common.model.{AnalyzerModel, RichMap}
import org.apache.flink.api.scala.DataSet

case class ModelingAnalyzerProcessor(override val filter: ReAnalyzer) extends AnalyzerProcessor[DataSet[RichMap]] {
  override def engine(): AnalyzerModel.AnalyzerModel = AnalyzerModel.MODELING
}
