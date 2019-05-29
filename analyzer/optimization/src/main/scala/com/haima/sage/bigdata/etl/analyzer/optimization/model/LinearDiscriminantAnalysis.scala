package com.haima.sage.bigdata.analyzer.optimization.model

import breeze.linalg._
import breeze.stats._
import com.haima.sage.bigdata.analyzer.ml.utils.MatrixUtils
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.pipeline._

/**
  * An Estimator that fits Linear Discriminant Analysis (currently not calculated in a distributed fashion),
  * and returns a transformer that projects into the new space
  *
  * Solves multi-class LDA via EigenVector decomposition
  *
  * @param numDimensions number of output dimensions to project to
  */
class LinearDiscriminantAnalysis(numDimensions: Int) extends Estimator[LinearDiscriminantAnalysis] with Serializable {
  /**
    * Currently this method works only on data that fits in local memory.
    * Hard limit of up to ~4B bytes of feature data due to max Java array length
    *
    * Solves multi-class LDA via Eigenvector decomposition
    *
    * "multi-class Linear Discriminant Analysis" or "Multiple Discriminant Analysis" by
    * C. R. Rao in 1948 (The utilization of multiple measurements in problems of biological classification)
    * http://www.jstor.org/discover/10.2307/2983775?uid=3739560&uid=2&uid=4&uid=3739256&sid=21106766791933
    *
    * Python implementation reference at: http://sebastianraschka.com/Articles/2014_python_lda.html
    *
    * @param data to train on.
    * @return A PipelineNode which can be called on new data.
    */
  def fit(data: DataSet[LabeledVector]): DataSet[LinearModel] = {
    computeLDA2(data)
  }

  def computeLDA2(value: DataSet[LabeledVector]): DataSet[LinearModel] = {
    import org.apache.flink.ml.math.Breeze._


    val labelVector = value.
      map(t => (t.label, t.vector.asBreeze.asInstanceOf[DenseVector[Double]], 1))

    val meanByClass: DataSet[(Double, DenseVector[Double], Double)] = labelVector.groupBy(0).
      reduce((l, r) => (l._1, l._2.+(r._2), l._3 + r._3)).map(t => (t._1, t._2 / t._3.toDouble, t._3.toDouble))

    val totalMean = meanByClass.map(t => (t._2 * t._3.toDouble, t._3)).
      reduce((f, s) => (f._1 + s._1, f._2 + s._2)).map(t => t._1 / t._2)
    val subMeanData: DataSet[(Double, DenseVector[Double], Int)] = labelVector.leftOuterJoin(meanByClass).where(0).equalTo(0).apply((f, s) => {
      (f._1, f._2.-(s._2), f._3)
    })

    val sW = subMeanData.map(v => {
      v._2 * v._2.t
    }).reduce(_ + _)
    //    val sW = subMeanData.cross(subMeanData).map(tuple => {
    //      if (tuple._1._1 == tuple._2._1) {
    //
    //        val m: DenseMatrix[Double] = tuple._1._2 * tuple._2._2.t
    //         m
    //      } else {
    //         DenseMatrix.zeros[Double](0, 0)
    //      }
    //
    //    }).filter(_.size > 0).reduce(_+ _)

    val sB = meanByClass.mapWithBcVariable(totalMean)((classes, total) => {
      (classes._1, classes._2 - total, classes._3)
    }).map(classes => {
      val m = classes._2
      (m * m.t) :* classes._3
    }).reduce(_ + _)
    val eigen = sW.mapWithBcVariable(sB)((sw, sb) => eig((inv(sw): DenseMatrix[Double]) * sb))

    eigen.mapWithBcVariable(eigen.map(_.eigenvalues.data))((e, d) => {
      val eigenVectors = (0 until e.eigenvectors.cols).map(e.eigenvectors(::, _).toDenseMatrix.t)
      val topEigenVectors = eigenVectors.zip(d).sortBy(x => -scala.math.abs(x._2)).map(_._1).take(numDimensions)
      LinearModel(DenseMatrix.horzcat(topEigenVectors: _*))
    })
  }

  def computeLDA(dataAndLabels: DataSet[LabeledVector]): DataSet[LinearModel] = {
    import org.apache.flink.ml.math.Breeze._
    val featuresByClass = dataAndLabels.groupBy(_.label).reduceGroup(values => {
      val aits = values.toArray
      val ites = aits.map(x => x.vector.asBreeze.asInstanceOf[DenseVector[Double]])
      val matrix: DenseMatrix[Double] = MatrixUtils.rowsToMatrix(ites)
      (aits.head.label, matrix)
    })
    val meanByClass = featuresByClass.map(f => (f._1, mean(f._2(::, *)))) // each mean is a row vector, not col
    val sW = featuresByClass.leftOuterJoin(meanByClass).where(0).equalTo(0).apply((f, s) => {
      val featuresMinusMean = f._2(*, ::) - s._2.t // row vector, not column
      featuresMinusMean.t * featuresMinusMean
    }).reduce(_ + _)

    val numByClass = featuresByClass.map(cf => (cf._1, cf._2.rows: Double))

    val totalMean = meanByClass.join(numByClass).where(0).equalTo(0).apply((f, s) => (f._2 * s._2, s._2))
      .reduce((f, s) => (f._1 + s._1, f._2 + s._2)).map(t => t._1 / t._2)
    /*dataAndLabels.map(_.vector).groupBy(_ => 0).reduceGroup(
    values => MatrixUtils.rowsToMatrix(values.map(x => x.asBreeze.asInstanceOf[DenseVector[Double]]).toArray)
  ).map(features => mean(features(::, *)))*/
    // val totalMean = mean(features(::, *)) // A row-vector, not a column-vector

    val sB = meanByClass.mapWithBcVariable(totalMean)((classes, total) => {
      (classes._1, classes._2 - total)
    }).join(numByClass).where(0).equalTo(0).apply((classes, classNum) => {
      val m = classes._2
      (m.t * m) :* classNum._2
    }).reduce(_ + _)
    val eigen = sW.mapWithBcVariable(sB)((sw, sb) => eig((inv(sw): DenseMatrix[Double]) * sb))

    eigen.mapWithBcVariable(eigen.map(_.eigenvalues.data))((e, d) => {
      val eigenVectors = (0 until e.eigenvectors.cols).map(e.eigenvectors(::, _).toDenseMatrix.t)
      val topEigenVectors = eigenVectors.zip(d).sortBy(x => -scala.math.abs(x._2)).map(_._1).take(numDimensions)
      LinearModel(DenseMatrix.horzcat(topEigenVectors: _*))
    })

  }

}