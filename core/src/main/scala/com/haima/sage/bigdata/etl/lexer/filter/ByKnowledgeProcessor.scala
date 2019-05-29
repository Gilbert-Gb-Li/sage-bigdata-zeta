package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.ByKnowledge
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}

/**
  * Created by Dell on 2017-08-28.
  */
class ByKnowledgeProcessor(override val filter: ByKnowledge) extends RuleProcessor[RichMap, RichMap, ByKnowledge] {

  private lazy val helper: KnowledgeUser = KnowledgeSingle(filter)

  override def process(event: RichMap): RichMap = {

    if (filter.isScript) {
      helper.byScript(event)
    } else {

      event.get(filter.value) match {
        case Some(v) =>
          helper.get(v) match {
            case Some(data) =>
              val fieldEvent = event
              data.filter(_._1 != filter.value).foldLeft(event)((e,prop) => {
                if (fieldEvent.contains(prop._1)) {
                  /*当有重名字段时*/
                  e.+((filter.value + "_" + prop._1)-> prop._2)
                } else {
                  e.+(prop._1-> prop._2)
                }
              })
            case _ =>
              event
          }
        case _ =>
          event
      }
    }

  }

  def close(): Unit = {
    helper.close()
  }
}
