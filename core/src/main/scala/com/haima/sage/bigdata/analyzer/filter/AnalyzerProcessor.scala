package com.haima.sage.bigdata.analyzer.filter

import com.haima.sage.bigdata.etl.common.model.filter.ReAnalyzer
import com.haima.sage.bigdata.etl.common.model.{Analyzer, AnalyzerModel, AnalyzerType, DataAnalyzer}
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import com.haima.sage.bigdata.etl.utils.{ClassUtils, Logger}


trait AnalyzerProcessor[T] extends RuleProcessor[T, List[T], ReAnalyzer] with Logger {


  def engine(): AnalyzerModel.AnalyzerModel


  private lazy val classes = if (AnalyzerModel.STREAMING == engine()) {
    //com.haima.sage.bigdata.etl.streaming.flink.analyzer
    ClassUtils.subClass(classOf[DataAnalyzer[_ <: Analyzer, T, T]],
      name = "com.haima.sage.bigdata.analyzer.streaming")
  } else {
    ClassUtils.subClass(classOf[DataAnalyzer[_ <: Analyzer, T, T]], name = "com.haima.sage.bigdata.analyzer.modeling")
  }
  /*构建输出处理流*/
  private lazy val analyzer: DataAnalyzer[_ <: Analyzer, T, T] =


    filter.analyzer match {
      case Some(real) =>
        try {
          val clazz = classes(real.getClass)
          /*
           * TODO Fixed ReAnalyzer only is support AnalyzerType.ANALYZER
           *
           * */
          engine() match {

            case AnalyzerModel.MODELING =>
              clazz.getConstructor(real.getClass, classOf[AnalyzerType.Type]).newInstance(real, AnalyzerType.ANALYZER)
            case _ =>
              clazz.getConstructor(real.getClass).newInstance(real)
          }

        } catch {
          case e: Exception =>
            e.printStackTrace()
            logger.error(s"flink make analyzer error", e)
            throw new UnsupportedOperationException(s"unknown analyzer for AnalyzerProcessor process :analyzer${filter.analyzer.orNull} or ref[${filter.ref.orNull}].")
        }
      case _ =>
        logger.error(s"""unknown analyzer for FlinkStream process :$analyzer""")
        throw new UnsupportedOperationException(s"unknown analyzer for FlinkStream process :${analyzer.getClass} .")

    }


  override def process(ds: T): List[T] = {
    analyzer.handle(ds)
  }
}


