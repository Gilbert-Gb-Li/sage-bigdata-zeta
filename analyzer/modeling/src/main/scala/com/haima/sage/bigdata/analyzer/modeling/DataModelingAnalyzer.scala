package com.haima.sage.bigdata.analyzer.modeling

import com.haima.sage.bigdata.etl.common.model._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * Created by evan on 17-8-29.
  */
abstract class DataModelingAnalyzer[CONF <: Analyzer](conf: CONF)
  extends DataAnalyzer[CONF, DataSet[RichMap], DataSet[RichMap]] {
  override def engine(): AnalyzerModel.Value = AnalyzerModel.MODELING

  def `type`: AnalyzerType.Type
  /**
    * 算法是否可以进行“生产模型”
    * @return
    */
  def modelAble: Boolean


  override def action(data: DataSet[RichMap]): DataSet[RichMap] = {
    if(modelAble){
      load()(data.getExecutionEnvironment) match {
        case m@Some(d)=>
          actionWithModel(data,m)
        case _=>
          actionWithModel(data,Option(modelling(data)))
      }
    }else{
      actionWithModel(data)
    }

  }

  /**
    * 加载模型
    * @return
    */
  def load()(implicit env:ExecutionEnvironment):Option[DataSet[RichMap]]

  /**
    * 训练模型
    * @param data
    * @return
    */
  def  modelling(data:DataSet[RichMap]):DataSet[RichMap]

  /**
    * 执行分析
    * @param data
    * @param model
    * @return
    */
  def  actionWithModel(data:DataSet[RichMap],model:Option[DataSet[RichMap]]=None):DataSet[RichMap]

  override lazy val isModel: Boolean = `type` == AnalyzerType.MODEL


}
