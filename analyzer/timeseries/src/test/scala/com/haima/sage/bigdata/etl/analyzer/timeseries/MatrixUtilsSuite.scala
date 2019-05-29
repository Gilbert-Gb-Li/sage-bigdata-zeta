package com.haima.sage.bigdata.analyzer.timeseries

import org.scalatest._
import org.scalatest.Matchers
import com.haima.sage.bigdata.analyzer.ml.utils.MatrixUtils._
import org.apache.flink.ml.math.{DenseMatrix,DenseVector,Breeze}
/**
  * Created by CaoYong on 2017/10/26.
  */
class MatrixUtilsSuite extends FunSuite with Matchers{
  test("modifying toBreeze version modifies original tensor") {
    val vec = DenseVector(1.0, 2.0, 3.0)
    val breezeVec = Breeze.Vector2BreezeConverter(vec).asBreeze
    breezeVec(1) = 4.0
    vec(1) should be  (4.0)

    val mat = DenseMatrix(1,2,Array(3.0,4.0))
    val breezeMat = Breeze.Matrix2BreezeConverter(mat).asBreeze
    breezeMat(0, 1) = 2.0
    mat(0, 1) should be (2.0)
  }
}
