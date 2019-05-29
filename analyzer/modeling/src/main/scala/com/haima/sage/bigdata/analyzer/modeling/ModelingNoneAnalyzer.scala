package com.haima.sage.bigdata.analyzer.modeling

import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, NoneAnalyzer, RichMap}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * Created by evan on 18-5-31.
  */
class ModelingNoneAnalyzer(override val conf: NoneAnalyzer, override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER) extends DataModelingAnalyzer[NoneAnalyzer](conf) {


  override def handle(data: DataSet[RichMap]): List[DataSet[RichMap]] = List(data)

  /**
    * 算法是否可以进行“生产模型”
    *
    * @return
    */
  override def modelAble: Boolean = ???

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
