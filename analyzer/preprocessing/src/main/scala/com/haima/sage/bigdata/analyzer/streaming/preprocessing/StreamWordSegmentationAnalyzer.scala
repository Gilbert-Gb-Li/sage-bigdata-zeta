package com.haima.sage.bigdata.analyzer.streaming.preprocessing

import com.haima.sage.bigdata.analyzer.model.preprocessing.TokenizerProcessor
import com.haima.sage.bigdata.analyzer.streaming.SimpleDataStreamAnalyzer
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{RichMap, WordSegmentationAnalyzer}

/**
  * Created by CHENGJI on 18-1-3
  */
class StreamWordSegmentationAnalyzer(override val conf: WordSegmentationAnalyzer) extends SimpleDataStreamAnalyzer[WordSegmentationAnalyzer, Any] {

  private val name=Constants.CONF.getString(s"app.analyzer.word-segmentation.${conf.tokenizer.name}")
  private lazy val tokenizer: TokenizerProcessor[_] = {
  Class.forName(name).getConstructor(conf.tokenizer.getClass)
      .newInstance(conf.tokenizer).asInstanceOf[TokenizerProcessor[_]]
  }


  /**
    * 将模型数据转换成算法的所需的结构类型
    *
    * @param model 原始的模型数据
    * @return 目标结构类型的模型数据
    */
  override def convert(model: Iterable[Map[String, Any]]): Any = {
    throw new NotImplementedError("WordSegmentation new not do convert for modeling")
  }

  /**
    * 算法预测逻辑实现
    *
    * @param in     输入流的一条记录
    * @param models 模型数据
    * @return RichMap
    */
  override def analyzing(in: RichMap, models: Any): RichMap = {
    in.get(conf.field) match {
      case Some(v) =>
        val dd = tokenizer.process(v.toString)
        in + ((conf.field + "_terms") -> dd)
      case _ =>
        in
    }


  }
}
