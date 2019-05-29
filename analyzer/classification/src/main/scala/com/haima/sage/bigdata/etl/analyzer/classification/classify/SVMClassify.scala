package com.haima.sage.bigdata.analyzer.classification.classify

import com.haima.sage.bigdata.analyzer.classification.model.SVMModel
import com.haima.sage.bigdata.etl.common.model.{RichMap, SVMAnalyzer}
import org.apache.flink.ml.math.{DenseVector, Vector}

import scala.collection.mutable.ArrayBuffer

trait SVMClassify {

  def conf: SVMAnalyzer

  private lazy val label = if (conf.classifyLabel.isEmpty || conf.classifyLabel == "") "label" else conf.classifyLabel

  def classify(in: RichMap, models: List[SVMModel]): RichMap = {
    val features: Vector = in.get(conf.features).map {
      case d: Vector =>
        d
      case d: List[Any@unchecked] =>
        DenseVector(d.map(_.toString.toDouble).toArray)
      case d: java.util.List[Any@unchecked] =>
        DenseVector(d.toArray().map(_.toString.toDouble))
      case d: Array[Double@unchecked] =>
        DenseVector(d)
      case d: ArrayBuffer[Double@unchecked] =>
        DenseVector(d.toArray)
    }.getOrElse(DenseVector())
    models match {
      case Nil =>
        throw UninitializedFieldError("svm to preview must be init model")
      case model :: Nil =>
        /*二分类*/
        in + (label -> (if (features.dot(model.weight) > model.threshold) model.positive
        else
          model.negative))
      case _ =>
        /*多分类,计算两次,第一次 one vs all others -> one vs one */
        val first = models.filter(model => model.positive == model.negative).zipWithIndex
        var rt: Any = null
        first.find(model => model._1.weight.dot(features) > model._1.threshold) match {
          case Some(f) =>
            var tails = models.filter(model => model.positive != model.negative)
            var flag = true
            var current = f._1.positive
            while (flag) {
              tails.find(model => model.positive == current) match {
                case Some(f1) =>
                  if (f1.weight.dot(features) > f1.threshold) {
                    tails = tails.dropWhile(model => model.positive == current || model.negative == f1.negative)
                  } else {
                    tails = tails.dropWhile(model => model.positive == current)
                    current = f1.negative
                  }
                case None =>
                  rt = current
                  flag = false
              }
            }
          case None =>
            var tails = models.filter(model => model.positive != model.negative)
            var flag = true
            var current = tails.head.positive
            while (flag) {
              while (flag) {
                tails.find(model => model.positive == current) match {
                  case Some(f1) =>
                    if (f1.weight.dot(features) > f1.threshold) {
                      tails = tails.dropWhile(model => model.positive == current || model.negative == f1.negative)
                    } else {
                      tails = tails.dropWhile(model => model.positive == current)
                      current = f1.negative
                    }
                  case None =>
                    rt = current
                    flag = false
                }
              }
            }
        }
        in + (label -> rt)
      // modelWeights.map(w => w._4.dot(features.getOrElse(DenseVector())))
    }
  }
}
