package com.haima.sage.bigdata.analyzer.optimization.modeling

import com.haima.sage.bigdata.etl.common.model.{LDAAnalyzer, RichMap}
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.StandardScaler
import org.junit.Test

/**
  * Created by lenovo on 2017/11/16.
  */
class ModelingLDAAnalyzerTest {
  val env = ExecutionEnvironment.getExecutionEnvironment

  @Test
  def testIrisData(): Unit = {


    // Uses the Iris flower dataset


    val irisData = env.readTextFile("/Users/zhhuiyan/workspace/sage-bigdata-etl/analyzer/optimization/src/test/resources/iris.data")
    val trainData = irisData.map(
      item => {
        val data = item.split(",").dropRight(1).map(_.toDouble)
        val lable = item match {
          case x if x.endsWith("Iris-setosa") => 1
          case x if x.endsWith("Iris-versicolor") => 2
          case x if x.endsWith("Iris-virginica") => 3
        }
        LabeledVector(lable, DenseVector(data))
      })

    val scaler = new StandardScaler()
    scaler.fit[LabeledVector](trainData)
    val scalerData = scaler.transform[LabeledVector, LabeledVector](trainData)
    val conf = LDAAnalyzer(2, "label")
    val analyzer = new ModelingLDAAnalyzer(conf)
    analyzer.modelling(scalerData.map(lv => RichMap(Map(
      "vector" -> lv.vector, "label" -> lv.label
    )))).collect().foreach(println)

  }


}
