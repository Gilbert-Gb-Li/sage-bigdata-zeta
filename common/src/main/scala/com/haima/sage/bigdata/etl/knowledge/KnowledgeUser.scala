package com.haima.sage.bigdata.etl.knowledge

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.ByKnowledge
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

trait KnowledgeUser {
  lazy val logger: Logger = LoggerFactory.getLogger(classOf[KnowledgeUser])

  def table: String

  def column: String

  def script: Option[String] = None


  /** 获取需要补足的数据,根据字段的具体值 */
  def get(value: Any): Option[Map[String, Any]]

  /** 获取需要补足的数据,根据字段的具体值 */
  def byScript(event: RichMap): RichMap

  /** 获取所有数据 */

  def getAll(): Iterable[Map[String, Any]]

  def close(): Unit

}

object KnowledgeSingle {

  def hash(by: ByKnowledge): Int = {

    if (by.isScript) {
      by.value.hashCode
    } else {
      by.column.hashCode
    }

  }

  def createPut(id: ByKnowledge, hash: Int): KnowledgeUser = {
    val user = Class.forName(Constants.CONF.getString("app.knowledge.helper")).asInstanceOf[Class[KnowledgeUser]]
      .getConstructor(classOf[String], classOf[String], classOf[Option[String]]).newInstance(id.id, id.column, Option(id.value))
    helper.put(id.id, (user, hash))
    user
  }

  private lazy val helper: mutable.Map[String, (KnowledgeUser, Int)] = new mutable.HashMap[String, (KnowledgeUser, Int)]()

  def apply(id: ByKnowledge): KnowledgeUser = {
    this.synchronized {
      val hashCode = hash(id)
      helper.get(id.id) match {
        case Some((user, `hashCode`)) =>
          user
        case Some((user, _)) =>
          user.close()
          createPut(id, hashCode)
        case _ =>
          createPut(id, hashCode)
      }
    }
  }

  def apply(_id: String): KnowledgeUser = {
    apply(ByKnowledge(_id))
  }
}