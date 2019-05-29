package com.haima.sage.bigdata.analyzer.classification.streaming

import com.haima.sage.bigdata.analyzer.classification.classify.SVMClassify
import com.haima.sage.bigdata.analyzer.classification.model.SVMModel
import com.haima.sage.bigdata.etl.common.model.{RichMap, SVMAnalyzer}
import com.haima.sage.bigdata.etl.streaming.flink.analyzer.{DataStreamAnalyzer, SimpleDataStreamAnalyzer}

import scala.collection.mutable


class StreamSVMAnalyzer(override val conf: SVMAnalyzer)
  extends SimpleDataStreamAnalyzer[SVMAnalyzer, List[SVMModel]] with SVMClassify {

  override def convert(models: Iterable[Map[String, Any]]): List[SVMModel] = {
    models.map(model => SVMModel(RichMap(model))).toList
  }

  override def analyzing(in: RichMap, models: List[SVMModel]): RichMap = {
    classify(in, models)
  }
}
