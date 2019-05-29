package com.haima.sage.bigdata.analyzer.optimization.modeling

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model.PCAAnalyzer
import com.haima.sage.bigdata.etl.common.model.filter.ReAnalyzer
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.ModelingAnalyzerProcessor
import org.apache.flink.api.scala._
import org.junit.Test

/**
  * Created by lenovo on 2017/11/16.
  */
class ModelingPCAAnalyzerTest {

  @Test
  def test(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    Constants.init("sage-analyzer-pca.conf")
    val conf = ReAnalyzer(Some(PCAAnalyzer(4, "zero", 0.7)))
    val processor = ModelingAnalyzerProcessor(conf)
    val ds: DataSet[RichMap] = env.fromElements(
      Map("a" -> 0.6, "b" -> 0.8, "c" -> 0.9, "d" -> 10),
      Map("a" -> 0.7, "e" -> 0.18, "c" -> 0.19, "d" -> 110),
      Map("a" -> 0.8, "f" -> 0.28, "c" -> 0.29, "3" -> 101),
      Map("a" -> 0.3, "g" -> 0.38, "c" -> 0.39, "d" -> 110),
      Map("a" -> 0.2, "h" -> 0.48, "c" -> 0.49, "d" -> 101),
      Map("a" -> 0.1, "b" -> 0.18, "c" -> 0.19, "f" -> 101),
      Map("a" -> 0.4, "b" -> 0.28, "g" -> 0.29, "d" -> 102),
      Map("h" -> 0.56, "b" -> 0.38, "c" -> 0.39, "d" -> 103))
    processor.process(ds).head.print()

  }


}
