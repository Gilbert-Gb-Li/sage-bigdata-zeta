package com.haima.sage.bigdata.etl.common.model

import com.haima.sage.bigdata.etl.common.model.filter.{ByKnowledge, Rule}
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}
import com.haima.sage.bigdata.etl.utils.{ClassUtils, Logger}

import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 2017/4/25.
  */
trait DataAnalyzer[CONF <: Analyzer, F, T] extends Serializable with Logger {

  /*加载知识库模型数据用例*/
  final lazy val modelUser: Option[KnowledgeUser] = conf.useModel.map(id => KnowledgeSingle(ByKnowledge(id)))

  /*具体算法配置接口*/
  def conf: CONF

  /*算法具体实现*/
  @throws[Exception]
  def action(data: F): T

  @throws[Exception]
  def handle(data: F): List[T] = {
    filter(action(data))
  }

  /*分析的引擎-stream or batch*/
  def engine(): AnalyzerModel.Value

  /* 结果是建模还是分析 */
  def isModel: Boolean

  private lazy val classes = if (AnalyzerModel.STREAMING == engine()) {
    ClassUtils.subClass(classOf[RuleProcessor[_, _, _]], name = "com.haima.sage.bigdata.analyzer.streaming.filter")
  } else {
    ClassUtils.subClass(classOf[RuleProcessor[_, _, _]], name = "com.haima.sage.bigdata.analyzer.modeling.filter")
  }

  private lazy val processors: Array[RuleProcessor[T, List[T], _ <: Rule]] = conf.filter.map(real => {
    classes(real.getClass).getConstructor(real.getClass).newInstance(real).
      asInstanceOf[RuleProcessor[T, List[T], _ <: Rule]]
  })


  def filter(data: T): List[T] = {
    if (conf.filter != null) {
      parse(processors.toList)(List(data))
    } else {
      List(data)
    }


  }


  @tailrec
  private def parse(rules: List[RuleProcessor[T, List[T], _ <: Rule]])(event: List[T]): List[T] = {

    rules match {
      case Nil =>
        event
      case head :: tail =>
        if (event.isEmpty) {
          List()
        } else {
          /*从前往后执行每一个过滤规则,第一组的结果,作为第二组的输入,组与组之间是串行*/
          parse(tail)(event.map(stream => head.process(stream)).reduce(_ ++: _))
        }


    }

  }

}
