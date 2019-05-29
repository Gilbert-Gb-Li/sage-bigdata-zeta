package com.haima.sage.bigdata.analyzer.optimization.model

;

import breeze.linalg.{Vector, eigSym, DenseMatrix => BDM}
import com.haima.sage.bigdata.etl.common.model.RichMap
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.{DataSet, createTypeInformation}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.DenseVector

/**
  * Created by jdj on 2017/11/1.
  */
object PCA {


  def cala(vector: Vector[Double]): List[(String, Double)] = {
    val size = vector.size
    var i, j = 0
    var x, y: Double = 0
    var list: List[(String, Double)] = Nil
    while (i < size) {
      x = vector(i)
      while (j < size) {

        y = vector(j)
        list = list.::(i + "_" + j, x * y)
        j += 1
      }
      j = 0
      i += 1
    }
    list
  }

  def fit(k: Int, inputDS: DataSet[DenseVector], minMatched: Double = 0): PCAModel = {
    val count = inputDS.count().toInt
    val featureCount = inputDS.first(1).collect()(0).size

    require(k > 0 && k < featureCount,
      s"Dimensions must be between 0 and ${featureCount}")
    //1.数据零均值处理  adjustedDS【DataSet【BreezeVector】】
    val adjustedVecters: DataSet[Vector[Double]] = inputDS.map(array => array.asBreeze)
    val reduceVecters: DataSet[Vector[Double]] = adjustedVecters.reduce(new ReduceFunction[Vector[Double]] {
      override def reduce(t: Vector[Double], t1: Vector[Double]): Vector[Double] = {
        t + t1
      }
    })
    val mean_vecter: DataSet[Vector[Double]] = reduceVecters.map(_ / count.toDouble)
    val mean: Vector[Double] = mean_vecter.collect()(0)
    val adjustedDS: DataSet[Vector[Double]] = adjustedVecters.map(v => v - mean)
    //    2.求0均值处理后数据的协方差
    //    2.1 求每个维度的期望值
    val reduceDS = adjustedDS.reduce(_ + _).map(_ / count.toDouble)
    val e_vecter = reduceDS.collect()(0)
    val eds = adjustedDS.map(_ / count.toDouble)
    val d: DataSet[List[(String, Double)]] = eds.map(vector => {
      cala(vector)
    })

    val tupeDS: DataSet[(String, Double)] = d.flatMap(list => list)
    val reduceGroupDs = tupeDS.groupBy(0).reduce((d1, d2) => {
      (d1._1, d1._2 + d2._2)
    })
    try {
      val reduceList: Seq[(String, Double)] = reduceGroupDs.collect()
      val map: Map[String, Double] = reduceList.toMap
      val g = BDM.eye[Double](featureCount)
      var i, j = 0
      var ex, ey, exy: Double = 0

      //构建协方差矩阵
      while (i < featureCount) {
        ex = e_vecter(i)
        while (j < featureCount) {
          ey = e_vecter(j)
          exy = map.get(i + "_" + j).get
          g(i, j) = ex * ey - exy
          j += 1
        }
        j = 0
        i += 1
      }
      val l = eigSym(g)

      var eigenvalues: breeze.linalg.DenseVector[Double] = null
      var eigvec: BDM[Double] = null
      if (minMatched != 0) {
        println("minMatched:" + minMatched)
        val data: Array[Double] = l.eigenvalues.data
        var calck = calcK(0, data, minMatched)
        if (calck == 0) {
          calck = data.length - 1
        }
        eigenvalues = l.eigenvalues(l.eigenvalues.length - calck to l.eigenvalues.length - 1)
        eigvec = l.eigenvectors(::, l.eigenvectors.cols - calck to l.eigenvectors.cols - 1)
        new PCAModel(calck, eigenvalues, eigvec)
      } else {
        eigenvalues = l.eigenvalues(l.eigenvalues.length - k to l.eigenvalues.length - 1)
        eigvec = l.eigenvectors(::, l.eigenvectors.cols - k to l.eigenvectors.cols - 1)
        new PCAModel(k, eigenvalues, eigvec)
      }

      //    //返回模型
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }

  def calcK(k: Int, data: Array[Double], minMatched: Double): Int = {
    if (k > data.length) {
      return 0
    }
    var i = 0
    val sumAll = data.sum
    var sumK: Double = 0
    while (i <= k) {
      sumK += data(data.length - i - 1)
      i += 1
    }
    if ((sumK / sumAll) > minMatched) {
      k
    } else {
      calcK(k + 1, data, minMatched)
    }

  }
}


class PCAModel(k: Int, eigenvalues: breeze.linalg.DenseVector[Double], eigenvecor: BDM[Double]) extends LinearModel(eigenvecor) {
  override def toMap(): RichMap = {

    RichMap(Map("k" -> k,
      "row" -> eigenvecor.rows,
      "col" -> eigenvecor.cols,
      "eigenvalues" -> eigenvalues.data.mkString(","),
      "eigenvecor" -> eigenvecor.data.mkString(",")
    ))
  }
}



