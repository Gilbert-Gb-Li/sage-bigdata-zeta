package com.haima.sage.bigdata.analyzer.preprocessing.modeling

import com.haima.sage.bigdata.analyzer.preprocessing.model.TokenizerProcessor
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, WordSegmentationAnalyzer}
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.DataModelingAnalyzer
import org.apache.flink.api.scala._

/**
  * Created by CHENGJI on 18-1-3
  */
class ModelingWordSegmentationAnalyzer(override val conf: WordSegmentationAnalyzer,
                                       override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER)
  extends DataModelingAnalyzer[WordSegmentationAnalyzer](conf) {
  private val name=Constants.CONF.getString(s"app.analyzer.word-segmentation.${conf.tokenizer.name}")
  lazy val tokenizer: TokenizerProcessor[_] = {
    Class.forName(name).getConstructor(conf.tokenizer.getClass)
      .newInstance(conf.tokenizer).asInstanceOf[TokenizerProcessor[_]]
  }

  override def action(data: DataSet[RichMap]): DataSet[RichMap] = {
    data.map(d => {
      d.get(conf.field) match {
        case Some(v) =>
          val dd = tokenizer.process(v.toString)
          d + ((conf.field + "_terms") -> dd)
        case _ =>
          d
      }

    })
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
