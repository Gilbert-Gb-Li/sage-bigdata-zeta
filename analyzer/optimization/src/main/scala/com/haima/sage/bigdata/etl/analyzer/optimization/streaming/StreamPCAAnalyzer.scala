package com.haima.sage.bigdata.analyzer.optimization.streaming

import com.haima.sage.bigdata.analyzer.optimization.model.PCAModel
import com.haima.sage.bigdata.analyzer.optimization.util.DataUtil._
import com.haima.sage.bigdata.etl.common.model.{PCAAnalyzer, RichMap}
import com.haima.sage.bigdata.etl.streaming.flink.analyzer.{DataStreamAnalyzer, SimpleDataStreamAnalyzer}
import org.apache.flink.ml.math._

import scala.collection.mutable

/**
  * Created by jdj on 2017/11/15.
  */
class StreamPCAAnalyzer(override val conf: PCAAnalyzer) extends SimpleDataStreamAnalyzer[PCAAnalyzer, PCAModel] {


  override def convert(model: Iterable[Map[String, Any]]): PCAModel = {
    toPCAModel(RichMap(model.head))
  }

  override def analyzing(in: RichMap, resultFuture: PCAModel): RichMap = {
    in.get("vector") match {
      case d: Vector =>
        val vector = resultFuture.transform(d)
        in + ("vector" -> vector)
      case _ =>
        logger.warn("flink warn not found usable data for do PCA opt")
        in
    }


  }
}
