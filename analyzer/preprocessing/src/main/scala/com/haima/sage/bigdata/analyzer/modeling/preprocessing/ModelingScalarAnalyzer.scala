package com.haima.sage.bigdata.analyzer.modeling.preprocessing

import com.haima.sage.bigdata.analyzer.modeling.DataModelingAnalyzer
import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, RichMap, ScalarAnalyzer}
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.math._

class ModelingScalarAnalyzer(override val conf: ScalarAnalyzer,
                             override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER) extends DataModelingAnalyzer[ScalarAnalyzer](conf) {




  import com.haima.sage.bigdata.analyzer.utils.preprocessing.VectorUtils._

  override def action(data: DataSet[RichMap]): DataSet[RichMap] = {

    data.map { d => {
      val vector = conf.cols.map {
        case (key, value) =>
          val rt = d.get(key) match {
            case Some(x: Int) =>
              DenseVector(Array(value.scalar(x)))
            case Some(x: Long) =>
              DenseVector(Array(value.scalar(x)))

            case Some(x: Float) =>
              DenseVector(Array(value.scalar(x)))

            case Some(x: Double) =>
              DenseVector(Array(value.scalar(x)))
            case Some(x: String) if x.matches("(\\d+.?\\d*|\\d*.?\\d+)") =>
              DenseVector(Array(value.scalar(x.toDouble)))

            case Some(x: Array[Double]) =>
              DenseVector(x.map(value.scalar))
            case Some(x: Vector) =>
              x
            case _ =>
              DenseVector(Array(0d))
          }
          rt

      }.foldLeft(SparseVector(0, Array(0), Array(0d)))((c, d) => {
        mergeVector(c, d).asInstanceOf[SparseVector]
      })

      add(d, vector)
    }
    }
  }

  /**
    * 算法是否可以进行“生产模型”
    *
    * @return
    */
  override def modelAble: Boolean = false

  /**
    * 加载模型
    *
    * @return
    */
override def load()(implicit env: ExecutionEnvironment): Option[DataSet[RichMap]] = ???

  /**
    * 训练模型
    *
    * @param data
    * @return
    */
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = ???

  /**
    * 执行分析
    *
    * @param data
    * @param model
    * @return
    */
  override def actionWithModel(data: DataSet[RichMap], model: Option[DataSet[RichMap]]): DataSet[RichMap] = ???
}
