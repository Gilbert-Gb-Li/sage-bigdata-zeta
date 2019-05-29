package com.haima.sage.bigdata.analyzer.preprocessing.utils

import com.haima.sage.bigdata.analyzer.utils.preprocessing.VectorUtils
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.junit.Test

class VectorUtilsTest {
  @Test
  def mergeTest(): Unit = {

    val sparseVector = SparseVector(10, Array(5), Array(1))
    val denseVector = DenseVector(Array(2))

    val rt = VectorUtils.mergeVector(sparseVector, denseVector)

    println(rt.size)
  }

}
