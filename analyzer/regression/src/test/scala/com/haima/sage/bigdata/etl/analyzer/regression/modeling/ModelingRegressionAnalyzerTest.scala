package com.haima.sage.bigdata.analyzer.regression.modeling

import breeze.stats.regression.leastSquares
import com.haima.sage.bigdata.analyzer.preprocessing.modeling.{ModelingScalarAnalyzer, ModelingVectorizationAnalyzer}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import org.apache.flink.api.scala.utils._
import org.apache.flink.ml.preprocessing.Splitter
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.math._
import org.junit.Test

class ModelingRegressionAnalyzerTest extends Serializable {
  /* 使用的jar包 */

  // private val env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, jar: _*)
  private val env = ExecutionEnvironment.getExecutionEnvironment
  Constants.init("sage-analyzer-svm.conf")
  //训练数据
  //二分类训练和预测用例
  protected val trainData: DataSet[RichMap] = env.fromElements(
    //    Map("label"->"正常","features"-> "5.1,3.5,1.4,0.2"),
    //    Map("label"->"正常","features"-> "4.9,3.0,1.4,0.2"),
    //    Map("label"->"正常","features"-> "4.7,3.2,1.3,0.2"),
    //    Map("label"->"正常","features"-> "4.6,3.1,1.5,0.2"),
    //    Map("label"->"正常","features"-> "5.0,3.6,1.4,0.2"),
    //    Map("label"->"正常","features"-> "5.4,3.9,1.7,0.4"),
    //    Map("label"->"正常","features"-> "4.6,3.4,1.4,0.3"),
    //    Map("label"->"正常","features"-> "5.0,3.4,1.5,0.2"),
    //    Map("label"->"正常","features"-> "4.4,2.9,1.4,0.2"),
    //    Map("label"->"正常","features"-> "4.9,3.1,1.5,0.1"),
    //    Map("label"->"正常","features"-> "5.4,3.7,1.5,0.2"),
    //    Map("label"->"正常","features"-> "4.8,3.4,1.6,0.2"),
    //    Map("label"->"正常","features"-> "4.8,3.0,1.4,0.1"),
    //    Map("label"->"正常","features"-> "4.8,3.0,1.4,0.1"),
    //    Map("label"->"正常","features"-> "5.8,4.0,1.2,0.2"),
    //    Map("label"->"正常","features"-> "5.7,4.4,1.5,0.4"),
    //    Map("label"->"正常","features"-> "5.4,3.9,1.3,0.4"),
    //    Map("label"->"正常","features"-> "5.1,3.5,1.4,0.3"),
    //    Map("label"->"正常","features"-> "5.7,3.8,1.7,0.3"),
    //    Map("label"->"正常","features"-> "5.1,3.8,1.5,0.3"),
    //    Map("label"->"正常","features"-> "5.4,3.4,1.7,0.2"),
    //    Map("label"->"正常","features"-> "5.1,3.7,1.5,0.4"),
    //    Map("label"->"正常","features"-> "4.6,3.6,1.0,0.2"),
    //    Map("label"->"正常","features"-> "5.1,3.3,1.7,0.5"),
    //    Map("label"->"正常","features"-> "4.8,3.4,1.9,0.2"),
    //    Map("label"->"正常","features"-> "5.0,3.0,1.6,0.2"),
    //    Map("label"->"正常","features"-> "5.0,3.4,1.6,0.4"),
    //    Map("label"->"正常","features"-> "5.2,3.5,1.5,0.2"),
    //    Map("label"->"正常","features"-> "5.2,3.4,1.4,0.2"),
    //    Map("label"->"正常","features"-> "4.7,3.2,1.6,0.2"),
    //    Map("label"->"正常","features"-> "4.8,3.1,1.6,0.2"),
    //    Map("label"->"正常","features"-> "5.4,3.4,1.5,0.4"),
    //    Map("label"->"正常","features"-> "5.2,4.1,1.5,0.1"),
    //    Map("label"->"正常","features"-> "5.5,4.2,1.4,0.2"),
    //    Map("label"->"正常","features"-> "4.9,3.1,1.5,0.1"),
    //    Map("label"->"正常","features"-> "5.0,3.2,1.2,0.2"),
    //    Map("label"->"正常","features"-> "5.5,3.5,1.3,0.2"),
    //    Map("label"->"正常","features"-> "4.9,3.1,1.5,0.1"),
    //    Map("label"->"正常","features"-> "4.4,3.0,1.3,0.2"),
    //    Map("label"->"正常","features"-> "5.1,3.4,1.5,0.2"),
    //    Map("label"->"正常","features"-> "5.0,3.5,1.3,0.3"),
    //    Map("label"->"正常","features"-> "4.5,2.3,1.3,0.3"),
    //    Map("label"->"正常","features"-> "4.4,3.2,1.3,0.2"),
    //    Map("label"->"正常","features"-> "5.0,3.5,1.6,0.6"),
    //    Map("label"->"正常","features"-> "5.1,3.8,1.9,0.4"),
    //    Map("label"->"正常","features"-> "4.8,3.0,1.4,0.3"),
    //    Map("label"->"正常","features"-> "5.1,3.8,1.6,0.2"),
    //    Map("label"->"正常","features"-> "4.6,3.2,1.4,0.2"),
    //    Map("label"->"正常","features"-> "5.3,3.7,1.5,0.2"),
    //    Map("label"->"正常","features"-> "5.0,3.3,1.4,0.2"),
    //    Map("label"->"异常","features"-> "7.0,3.2,4.7,1.4"),
    //    Map("label"->"异常","features"-> "6.4,3.2,4.5,1.5"),
    //    Map("label"->"异常","features"-> "6.9,3.1,4.9,1.5"),
    //    Map("label"->"异常","features"-> "5.5,2.3,4.0,1.3"),
    //    Map("label"->"异常","features"-> "6.5,2.8,4.6,1.5"),
    //    Map("label"->"异常","features"-> "5.7,2.8,4.5,1.3"),
    //    Map("label"->"异常","features"-> "6.3,3.3,4.7,1.6"),
    //    Map("label"->"异常","features"-> "4.9,2.4,3.3,1.0"),
    //    Map("label"->"异常","features"-> "6.6,2.9,4.6,1.3"),
    //    Map("label"->"异常","features"-> "5.2,2.7,3.9,1.4"),
    //    Map("label"->"异常","features"-> "5.0,2.0,3.5,1.0"),
    //    Map("label"->"异常","features"-> "5.9,3.0,4.2,1.5"),
    //    Map("label"->"异常","features"-> "6.0,2.2,4.0,1.0"),
    //    Map("label"->"异常","features"-> "6.1,2.9,4.7,1.4"),
    //    Map("label"->"异常","features"-> "5.6,2.9,3.6,1.3"),
    //    Map("label"->"异常","features"-> "6.7,3.1,4.4,1.4"),
    //    Map("label"->"异常","features"-> "5.6,3.0,4.5,1.5"),
    //    Map("label"->"异常","features"-> "5.8,2.7,4.1,1.0"),
    //    Map("label"->"异常","features"-> "6.2,2.2,4.5,1.5"),
    //    Map("label"->"异常","features"-> "5.6,2.5,3.9,1.1"),
    //    Map("label"->"异常","features"-> "5.9,3.2,4.8,1.8"),
    //    Map("label"->"异常","features"-> "6.1,2.8,4.0,1.3"),
    //    Map("label"->"异常","features"-> "6.3,2.5,4.9,1.5"),
    //    Map("label"->"异常","features"-> "6.1,2.8,4.7,1.2"),
    //    Map("label"->"异常","features"-> "6.4,2.9,4.3,1.3"),
    //    Map("label"->"异常","features"-> "6.6,3.0,4.4,1.4"),
    //    Map("label"->"异常","features"-> "6.8,2.8,4.8,1.4"),
    //    Map("label"->"异常","features"-> "6.7,3.0,5.0,1.7"),
    //    Map("label"->"异常","features"-> "6.0,2.9,4.5,1.5"),
    //    Map("label"->"异常","features"-> "5.7,2.6,3.5,1.0"),
    //    Map("label"->"异常","features"-> "5.5,2.4,3.8,1.1"),
    //    Map("label"->"异常","features"-> "5.5,2.4,3.7,1.0"),
    //    Map("label"->"异常","features"-> "5.8,2.7,3.9,1.2"),
    //    Map("label"->"异常","features"-> "6.0,2.7,5.1,1.6"),
    //    Map("label"->"异常","features"-> "5.4,3.0,4.5,1.5"),
    //    Map("label"->"异常","features"-> "6.0,3.4,4.5,1.6"),
    //    Map("label"->"异常","features"-> "6.7,3.1,4.7,1.5"),
    //    Map("label"->"异常","features"-> "6.3,2.3,4.4,1.3"),
    //    Map("label"->"异常","features"-> "5.6,3.0,4.1,1.3"),
    //    Map("label"->"异常","features"-> "5.5,2.5,4.0,1.3"),
    //    Map("label"->"异常","features"-> "5.5,2.6,4.4,1.2"),
    //    Map("label"->"异常","features"-> "6.1,3.0,4.6,1.4"),
    //    Map("label"->"异常","features"-> "5.8,2.6,4.0,1.2"),
    //    Map("label"->"异常","features"-> "5.0,2.3,3.3,1.0"),
    //    Map("label"->"异常","features"-> "5.6,2.7,4.2,1.3"),
    //    Map("label"->"异常","features"-> "5.7,3.0,4.2,1.2"),
    //    Map("label"->"异常","features"-> "5.7,2.9,4.2,1.3"),
    //    Map("label"->"异常","features"-> "6.2,2.9,4.3,1.3"),
    //    Map("label"->"异常","features"-> "5.1,2.5,3.0,1.1"),
    //    Map("label"->"异常","features"-> "5.7,2.8,4.1,1.3")
    //  )
//    Map("features" -> DenseVector(0.9988732405174987, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.9987919942916673, 0.9999843227991471, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.9987804233839391, 0.9995744520872002, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.9991579108451372, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.9991992208344512, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.9991314878889358, 0.9999206704337604, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9992562856509822, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9992562856509822, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9992562856509822, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9988909074419934, 0.9976934167150409, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9989748482735932, 0.9999921653547099, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9988811614816755, 0.9998931308097386, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9988752312544874, 0.9998876421168801, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9992562856509822, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9990596465149666, 0.9998517765399618, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 1)
//  )
//    Map("features" -> DenseVector(0.1, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.2, 0.9999843227991471, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.3, 0.9995744520872002, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.4, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.5, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 2),
//    Map("features" -> DenseVector(0.6, 0.9999206704337604, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.7, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.8, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(0.9, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(1.0, 0.9976934167150409, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0), "label" -> 1),
//    Map("features" -> DenseVector(1.1, 0.9999921653547099, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(1.2, 0.9998931308097386, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(1.3, 0.9998876421168801, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(1.4, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "label" -> 1),
//    Map("features" -> DenseVector(1.5, 0.9998517765399618, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0), "label" -> 1)
    Map("features" -> DenseVector(1.5 ), "label" -> 3.0),
    Map("features" -> DenseVector(2.0 ), "label" -> 4.0),
    Map("features" -> DenseVector(2.5 ), "label" -> 5.0),
    Map("features" -> DenseVector(3.0 ), "label" -> 6.0),
    Map("features" -> DenseVector(3.5 ), "label" -> 7.0),
    Map("features" -> DenseVector(4.0 ), "label" -> 8.0),
    Map("features" -> DenseVector(5.0 ), "label" -> 10.0),
    Map("features" -> DenseVector(8.0 ), "label" -> 16.0),
    Map("features" -> DenseVector(8.5 ), "label" -> 17.0),
    Map("features" -> DenseVector(9.0 ), "label" -> 18.0)
  )
  protected val unpredictData: DataSet[RichMap] = env.fromElements(
    Map("features" -> DenseVector(0.9988732405174987, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9987919942916673, 0.9999843227991471, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9987804233839391, 0.9995744520872002, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0)),
    Map("features" -> DenseVector(0.9991579108451372, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9991992208344512, 0.9999843224130757, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9991314878889358, 0.9999206704337604, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9992562856509822, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9992562856509822, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9992562856509822, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9988909074419934, 0.9976934167150409, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0)),
    Map("features" -> DenseVector(0.9989748482735932, 0.9999921653547099, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0)),
    Map("features" -> DenseVector(0.9988811614816755, 0.9998931308097386, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9988752312544874, 0.9998876421168801, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9992562856509822, 0.9965773460288905, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)),
    Map("features" -> DenseVector(0.9990596465149666, 0.9998517765399618, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0))
  )

  @Test
  def train(): Unit = { //训练模型
    val conf = RegressionAnalyzer(1, 0.002, 100, 0.001, "label", "features")
    // val processor = ModelingAnalyzerProcessor(conf)
    val processor = new ModelingRegressionAnalyzer(conf, AnalyzerType.MODEL)
    assert(processor.engine() == AnalyzerModel.MODELING)
      //将测试集分为训练集和测试集
//      val trainTestData= Splitter.trainTestSplit(trainData,0.6,false)
//    val trainingData = trainTestData.training
//    val testingData = trainTestData.testing
//    var d1=trainingData.collect()
//    var d2=testingData.collect()
//    println("trainingData:"+d1)
//    println("-----------------")
//    println("testingData:"+d2)
    val result = processor.modelling(trainData)
    val result1 = result.collect()
    println("单元测试结果：",result1)
//    val left = result.map(v=>{
//      val vector = v("features").asInstanceOf[DenseVector]
//      val label = v("label").toString
//      (vector,label)
//    })
//    val a= left.collect()
//    val right = trainData.map(v=>{
//      val vector = v("features").asInstanceOf[DenseVector]
//      val label = v("label").toString
//      (vector,label)
//    })
//    val b= right.collect()
//    val result3=left.join(right).where(0).equalTo(0).apply{
//      (l,r)=> (r._1, r._2, l._2)
//    }


//    println("最后结果",result3.collect())
//    val result = processor.modelling(trainData).collect()
//    val result = processor.action(trainingData).collect()
//    val a = result
//    result.foreach(v => {
//      println("trainingData:中的V",v)
//    })

//    val trainTetrainingData:stData: DataSet[trainData] = Splitter.trainTestSplit(trainData)
//    val a_ = result1.sample(false,0.5).collect()
////    trainData.
////    val bb=a_.collect()
////    val re = bb
//    a_.foreach(v => {
//      println("抽样后的数据：",v)
//    })
//做一次测试：
//  val testresult = processor.train(testingData).collect()
//    val testre = result
//    result.foreach(v => {
//      println("testingData:中的V",v)
//    })




//      val conf = ReAnalyzer(Some(SVMAnalyzer(0.002, 0.1, 100, 100, "label", "features", None, Array(), None)))
//      val processor = ModelingAnalyzerProcessor(conf)
//      //assert(processor.engine() == AnalyzerModel.MODELING)
//      val result = processor.process(unpredictData).head
//      println(result.print())


  }

  @Test
  def train2(): Unit = {
    val vectorization = VectorizationAnalyzer("task_id", SameWeightVectorization())
    val scalar = ScalarAnalyzer(Array("ids_number" -> NoThing(), "ods_number" -> NoThing()))

    val regression = RegressionAnalyzer(1, 1.0, 100, 1.0, "usedtime", "vector")


    val vectorizationProcessor = new ModelingVectorizationAnalyzer(vectorization)
    val scalarProcessor = new ModelingScalarAnalyzer(scalar)
    val regressionProcessor = new ModelingRegressionAnalyzer(regression, AnalyzerType.ANALYZER)
    val data:DataSet[RichMap] = env.readTextFile("/Users/zhhuiyan/Downloads/local-json-sql.json").map(new Mapper() {}.mapper.readValue[Map[String, Any]](_))
    //vectorizationProcessor.action(data).print()
    regressionProcessor.action(scalarProcessor.action(vectorizationProcessor.action(data))).print()


  }

  @Test
  def predict(): Unit = {
    val data:Seq[RichMap] = Seq(
      Map("label" -> 10.7949, "vector" -> DenseVector(Array(0.2714))),
      Map("label" -> 10.6426, "vector" -> DenseVector(Array(0.1008))),
      Map("label" -> 10.5603, "vector" -> DenseVector(Array(0.5078))),
      Map("label" -> 12.8707, "vector" -> DenseVector(Array(0.5856))),
      Map("label" -> 10.7026, "vector" -> DenseVector(Array(0.7629))),
      Map("label" -> 9.8571, "vector" -> DenseVector(Array(0.0830))),
      Map("label" -> 10.5001, "vector" -> DenseVector(Array(0.6616))),
      Map("label" -> 11.2063, "vector" -> DenseVector(Array(0.5170))),
      Map("label" -> 9.1892, "vector" -> DenseVector(Array(0.1710))),
      Map("label" -> 12.2408, "vector" -> DenseVector(Array(0.9386))),
      Map("label" -> 11.0307, "vector" -> DenseVector(Array(0.5905))),
      Map("label" -> 10.1369, "vector" -> DenseVector(Array(0.4406))),
      Map("label" -> 10.7609, "vector" -> DenseVector(Array(0.9419))),
      Map("label" -> 12.5328, "vector" -> DenseVector(Array(0.6559))),
      Map("label" -> 13.3560, "vector" -> DenseVector(Array(0.4519))),
      Map("label" -> 14.7424, "vector" -> DenseVector(Array(0.8397))),
      Map("label" -> 11.1057, "vector" -> DenseVector(Array(0.5326))),
      Map("label" -> 11.6157, "vector" -> DenseVector(Array(0.5539))),
      Map("label" -> 11.5744, "vector" -> DenseVector(Array(0.6801))),
      Map("label" -> 11.1775, "vector" -> DenseVector(Array(0.3672))),
      Map("label" -> 9.7991, "vector" -> DenseVector(Array(0.2393))),
      Map("label" -> 9.8173, "vector" -> DenseVector(Array(0.5789))),
      Map("label" -> 12.5642, "vector" -> DenseVector(Array(0.8669))),
      Map("label" -> 9.9952, "vector" -> DenseVector(Array(0.4068))),
      Map("label" -> 8.4354, "vector" -> DenseVector(Array(0.1126))),
      Map("label" -> 13.7058, "vector" -> DenseVector(Array(0.4438))),
      Map("label" -> 10.6672, "vector" -> DenseVector(Array(0.3002))),
      Map("label" -> 11.6080, "vector" -> DenseVector(Array(0.4014))),
      Map("label" -> 13.6926, "vector" -> DenseVector(Array(0.8334))),
      Map("label" -> 9.5261, "vector" -> DenseVector(Array(0.4036))),
      Map("label" -> 11.5837, "vector" -> DenseVector(Array(0.3902))),
      Map("label" -> 11.5831, "vector" -> DenseVector(Array(0.3604))),
      Map("label" -> 10.5038, "vector" -> DenseVector(Array(0.1403))),
      Map("label" -> 10.9382, "vector" -> DenseVector(Array(0.2601))),
      Map("label" -> 9.7325, "vector" -> DenseVector(Array(0.0868))),
      Map("label" -> 12.0113, "vector" -> DenseVector(Array(0.4294))),
      Map("label" -> 9.9219, "vector" -> DenseVector(Array(0.2573))),
      Map("label" -> 10.0963, "vector" -> DenseVector(Array(0.2976))),
      Map("label" -> 11.9999, "vector" -> DenseVector(Array(0.4249))),
      Map("label" -> 12.0442, "vector" -> DenseVector(Array(0.1192)))
    )


    val data2 = Seq(
      Map("label" -> 5, "vector" -> DenseVector(Array(1))),
      Map("label" -> 6, "vector" -> DenseVector(Array(2))),
      Map("label" -> 7, "vector" -> DenseVector(Array(3))),
      Map("label" -> 8, "vector" -> DenseVector(Array(4))),
      Map("label" -> 9, "vector" -> DenseVector(Array(5))),
      Map("label" -> 10, "vector" -> DenseVector(Array(6))),
      Map("label" -> 11, "vector" -> DenseVector(Array(7))),
      Map("label" -> 12, "vector" -> DenseVector(Array(8))),
      Map("label" -> 13, "vector" -> DenseVector(Array(9))),
      Map("label" -> 14, "vector" -> DenseVector(Array(10))),
      Map("label" -> 15, "vector" -> DenseVector(Array(11))),
      Map("label" -> 16, "vector" -> DenseVector(Array(12))),
      Map("label" -> 17, "vector" -> DenseVector(Array(13))))

    // EasyPlot.ezplot(data.map(_("label").asInstanceOf[Double]).toArray)


    //
    //
    // TimeUnit.HOURS.sleep(1)


    /*
    * y1=ax1+b
    * y2=ax2+b
    * 5=a1+b
    * 6=a2+b
    *
    * */


    //分类预测
    val conf = RegressionAnalyzer(1, 0.01, 100, 0.00, "label", "vector", "", None)
    val processor = new ModelingRegressionAnalyzer(conf, AnalyzerType.ANALYZER)
    assert(processor.engine() == AnalyzerModel.MODELING)
   val set= env.fromElements(data: _*)
    set.writeAsText("./test7.txt",WriteMode.OVERWRITE)
    val result = processor.action(set)
    println(result.print())
  }

}
