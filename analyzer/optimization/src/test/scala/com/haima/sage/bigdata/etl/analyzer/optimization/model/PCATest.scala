package com.haima.sage.bigdata.analyzer.optimization.model

import org.apache.flink.api.scala._
import org.apache.flink.ml.math._
import org.junit.Test

/**
  * Created by lenovo on 2017/11/13.
  */
class PCATest {


  @Test
  def test(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val v1 = DenseVector(1.2, 2, 2, 2.277, 1.312, 1.5, 1.6, 1.67, 1.8, 1.2, 1.3)
    val v2 = DenseVector(1.1, 2, 2, 1.271, 1.312, 1.5, 1.6, 1.7, 1.8, 1.52, 1.2)
    val v3 = DenseVector(1.3, 2, 3, 1.2, 177.3, 112.5, 1.6, 1.7, 1.78, 1.42, 1.4)
    val v4 = DenseVector(1.4, 2, 244, 1.266, 1.322, 11.54, 1.26, 1.87, 1.8, 1.32, 1.5)
    val v5 = DenseVector(1.5, 2, 255, 1.255, 1.32, 1.53, 1.16, 1.7, 1.8, 1.212, 1.1)
    val v6 = DenseVector(1.11, 2, 211, 1.244, 1.32, 1.52, 1.26, 1.7, 1.8, 1.2, 1.7)
    val v7 = DenseVector(1.2, 222, 2, 1.233, 1.33, 1.51, 1.36, 1.7, 1.8, 1.2, 1.412)
    val v8 = DenseVector(1.4, 2, 233, 1.222, 1.34, 1.57, 1.46, 1.7, 1.8, 1.2, 1.33)
    val v9 = DenseVector(1.3, 2, 244, 1.211, 1.35, 1.56, 1.6, 1.7, 1.8, 1.2, 1.112)
    val list = List(v1, v2, v3, v4, v5, v6, v7, v8, v9)
    val ds = env.fromCollection(list)
    val pcamodel = PCA.fit(3, ds)
    val dd = ds.map(vector => {
      pcamodel.transform(vector)
    })
    println("--------------------------------------------------------------------------------------------------------")
    dd.print()
  }
}
