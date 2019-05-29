package com.haima.sage.bigdata.analyzer.aggregation.util

import com.haima.sage.bigdata.analyzer.aggregation.model.Pattern
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}

/**
  * Created by wxn on 2017/12/28.
  */
object DataUtil extends Serializable {
  //根据知识库Id，返回模型
  def getModel(knowledgeId: Option[String]): List[RichMap] = {
    /*knowledgeId match {
      case Some(id: String) if id != null && id.trim != null =>
        val helper: KnowledgeUser = KnowledgeSingle(id)
        helper.getAll().map(d => {
          (Pattern(d("pattern").toString), d("pattern_key").toString)
        }).toList.sortWith((f, s) => f._1.length > s._1.length)


      case _ =>
        List()
    }*/

    knowledgeId match {
      case Some(id: String) if id != null && id.trim != null =>
        val helper: KnowledgeUser = KnowledgeSingle(id)
        helper.getAll().map(RichMap(_)).toList
      case _ =>
        List()
    }

  }

  def toLogReducerModel(data: RichMap): (Pattern, String) = {
    (Pattern(data("pattern").toString), data("pattern_key").toString)
  }
}
