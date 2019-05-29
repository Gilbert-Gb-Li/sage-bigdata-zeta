package com.haima.sage.bigdata.analyzer.streaming

import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import org.apache.flink.streaming.api.scala.DataStream

/**
  * Created by ChengJi on 2017/4/25.
  */
class NoneDataStreamAnalyzer(override val conf: NoneAnalyzer) extends SimpleDataStreamAnalyzer[NoneAnalyzer, RichMap] {

  @throws[Exception]
  override def action(data: DataStream[RichMap]): DataStream[RichMap] = {
    data
  }

  /**
    * 将模型数据转换成算法的所需的结构类型
    *
    * @param model 原始的模型数据
    * @return 目标结构类型的模型数据
    */
  override def convert(model: Iterable[Map[String, Any]]): RichMap = {
    throw new NotImplementedError("none analyzer not use convert")
  }

  /**
    * 算法预测逻辑实现
    *
    * @param in     中间数据1
    * @param models 模型数据
    * @return 分析后的中间数据2
    */
  override def analyzing(in: RichMap, models: RichMap): RichMap = {
    in
  }
}
