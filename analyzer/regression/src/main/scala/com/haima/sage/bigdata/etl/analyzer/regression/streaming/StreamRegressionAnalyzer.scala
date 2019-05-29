package com.haima.sage.bigdata.analyzer.streaming.regression

import com.haima.sage.bigdata.analyzer.regression.model.PolynomialCalculate
import com.haima.sage.bigdata.analyzer.streaming.SimpleDataStreamAnalyzer
import com.haima.sage.bigdata.etl.common.model.{RegressionAnalyzer, RichMap}
import org.apache.flink.ml.common.WeightVector
import org.apache.flink.ml.math.{Breeze, DenseVector, SparseVector, Vector}
import org.apache.flink.ml.preprocessing.PolynomialFeatures


class StreamRegressionAnalyzer(override val conf: RegressionAnalyzer) extends SimpleDataStreamAnalyzer[RegressionAnalyzer, Option[WeightVector]] {

  override def convert(model: Iterable[Map[String, Any]]): Option[WeightVector] = {
    model.find(item => {
      val t = item.get("type")
      t.isDefined && t.exists(_ != null)
    }) match {
      case Some(data) =>
        logger.warn(s"datashi:$data")
        logger.warn(s"data.get(type):${data.get("type")}")
        data.get("type") match {
          case Some("sparse") =>
            Some(WeightVector(SparseVector(data("size").toString.toInt, data("indices").toString.split(",").map(_.toInt),
              data("data").toString.split(",").map(_.toDouble)), data("intercept").toString.toDouble))
          case Some("dense") =>
            Some(WeightVector(DenseVector(data("data").toString.split(",").map(_.toDouble)), data("intercept").toString.toDouble))
          case obj =>
            logger.warn(s"real:$obj")
            None
        }
      case _ =>
        None
    }

  }

  override def analyzing(in: RichMap, weightVector: Option[WeightVector]): RichMap = {
    if (weightVector == null || weightVector.isEmpty) {
      throw new ExceptionInInitializerError("weightVector为空！")
    }


    val polynomialBase = PolynomialFeatures()
    polynomialBase.setDegree(conf.degree)

    def predict(value: Vector, model: WeightVector): Double = {
      import Breeze._
      val WeightVector(weights, weight0) = model
      val dotProduct = value.asBreeze.dot(weights.asBreeze)
      dotProduct + weight0
    }

    in.get(conf.features) match {
      case Some(d: Vector) =>
        in + (conf.label ->
          predict(PolynomialCalculate.calculatePolynomial[Vector](conf.degree, d), weightVector.get))
      case d =>
        logger.warn(s"data is not vector type,class is ${d.getClass}")
        in
    }
  }
}
