package com.haima.sage.bigdata.analyzer.classification.model

import com.haima.sage.bigdata.etl.common.model.RichMap
import org.apache.flink.ml.math.DenseVector

case class SVMModel(positive: String, negative: String, weight: DenseVector=null, threshold: Double = 0)extends Serializable {


  def toMap: RichMap = {
    RichMap(Map("positive" -> positive, "negative" -> negative, "weight" -> weight.data.mkString(","), "threshold" -> threshold))

  }
}

object SVMModel {
  def apply(model: RichMap): SVMModel = {
    SVMModel(model.get("positive").map(_.toString).orNull,
      model.get("negative").map(_.toString).orNull,
      DenseVector(model.get("weight").orNull.toString.split(",").map(_.toDouble)),
      model.get("threshold").map(_.toString.toDouble).getOrElse(0d))

  }
}