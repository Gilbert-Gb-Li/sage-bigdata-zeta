package com.haima.sage.bigdata.analyzer.optimization.model

import breeze.linalg.{DenseMatrix => BDM}
import com.haima.sage.bigdata.etl.common.model.RichMap
import org.apache.flink.ml.math.Breeze._

/**
  * 降维操作
  *
  * @param eigen 特征矩阵
  */
case class LinearModel(eigen: BDM[Double]) extends Serializable {

  private lazy val eigenT = eigen.t



  def toMap: RichMap = {

    RichMap(Map("rows" -> eigen.rows,
      "cols" -> eigen.cols,
      "data" -> eigen.data.mkString("[", ",", "]")
    ))
  }


  def transform(data: org.apache.flink.ml.math.Vector): org.apache.flink.ml.math.Vector = {
    (eigenT * data.asBreeze).fromBreeze
  }
}
