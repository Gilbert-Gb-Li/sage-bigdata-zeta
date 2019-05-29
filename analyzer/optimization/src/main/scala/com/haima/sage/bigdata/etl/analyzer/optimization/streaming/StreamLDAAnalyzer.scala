package com.haima.sage.bigdata.analyzer.optimization.streaming

import breeze.linalg.DenseMatrix
import com.haima.sage.bigdata.analyzer.optimization.model.LinearModel
import com.haima.sage.bigdata.etl.common.model.{LDAAnalyzer, RichMap}
import com.haima.sage.bigdata.etl.streaming.flink.analyzer.SimpleDataStreamAnalyzer
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.ml.math._

/**
  * Created by jdj on 2017/11/15.
  */
class StreamLDAAnalyzer(override val conf: LDAAnalyzer) extends SimpleDataStreamAnalyzer[LDAAnalyzer, LinearModel] {


  override def convert(model: Iterable[Map[String, Any]]): LinearModel = {
    val richMap = model.head
    val mapper = new Mapper {}.mapper
    LinearModel(new DenseMatrix[Double](richMap("rows").toString.toInt,
      richMap("cols").toString.toInt
      , mapper.readValue[Array[Double]](richMap("data").toString)))

  }

  override def analyzing(in: RichMap, model: LinearModel): RichMap = {
    in.get("vector") match {
      case d: Vector =>
        val vector = model.transform(d)
        in + ("vector" -> vector)
      case _ =>
        logger.warn("flink warn not found usable data for do PCA opt")
        in
    }


  }
}
