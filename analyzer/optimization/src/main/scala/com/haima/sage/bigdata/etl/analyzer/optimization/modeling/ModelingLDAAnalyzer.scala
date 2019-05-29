package com.haima.sage.bigdata.analyzer.optimization.modeling

import breeze.linalg.DenseMatrix
import com.haima.sage.bigdata.analyzer.optimization.model.{LinearDiscriminantAnalysis, LinearModel}
import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, LDAAnalyzer, RichMap}
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.DataModelingAnalyzer
import com.haima.sage.bigdata.etl.utils.{Logger, Mapper}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.ml._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math._

/**
  * Created by jdj on 2017/11/14.
  */
class ModelingLDAAnalyzer(override val conf: LDAAnalyzer,
                          override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER) extends DataModelingAnalyzer[LDAAnalyzer](conf) with Logger {

  /**
    * 算法是否可以进行“生产模型”
    *
    * @return
    */
  override def modelAble: Boolean = true

  /**
    * 加载模型
    *
    * @return
    */
  override def load()(implicit env: ExecutionEnvironment): Option[DataSet[RichMap]] = {
    val knowledgeId = conf.useModel.get
    val helper: KnowledgeUser = KnowledgeSingle(knowledgeId)
    //id对应就是模型库的表名
    Option(env.fromCollection(helper.getAll().map(RichMap(_))))

  }

  /**
    * 训练模型
    *
    * @param data
    * @return
    */
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = {
    val lda = new LinearDiscriminantAnalysis(conf.dimensions)
    val labels = data.map(_.get(conf.label).map(_.toString).orNull).filter(_ != null).distinct(d=>d).zipWithIndex.groupBy(t=>0).reduceGroup(_.toMap)
    lda.fit(data.mapWithBcVariable(labels) {
      (d, label) =>
        LabeledVector(label.find(_._2 == d(conf.label)).map(_._1.toDouble).getOrElse(0d), d("vector").asInstanceOf[Vector])
    }
    ).map(_.toMap)


  }

  private lazy val mapper = new Mapper {}.mapper

  def from(richMap: RichMap): LinearModel = {

    LinearModel(new DenseMatrix[Double](richMap("rows").toString.toInt,
      richMap("cols").toString.toInt
      , mapper.readValue[Array[Double]](richMap("data").toString)))

  }

  /**
    * 执行分析
    *
    * @param data
    * @param model
    * @return
    */
  override def actionWithModel(data: DataSet[RichMap], model: Option[DataSet[RichMap]]): DataSet[RichMap] = model match {
    case Some(m) =>
      data.mapWithBcVariable(m.map(t => from(t)))((data, model) => {
        data.get("vector") match {
          case Some(v: Vector) =>
            data + ("vector" -> model.transform(v))
          case _ =>
            logger.warn(s"not found an available vector data")
            data
        }

      })

    case None =>
      logger.warn(s"not found an available vector data")
      throw new ExceptionInInitializerError(s"not found an available vector data")
  }
}
