package com.haima.sage.bigdata.etl.store.rules

import java.sql.ResultSet

import com.haima.sage.bigdata.etl.common.model.{ParserWrapper, RuleGroup}
import com.haima.sage.bigdata.etl.store.DBStore
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by zhhuiyan on 2015/5/28.
  */
 class DBRuleGroupStore extends RuleGroupStore with DBStore[RuleGroup,String] {
  override val logger: Logger = LoggerFactory.getLogger(classOf[DBRuleUnitStore])
  val CREATE_TABLE: String = "CREATE TABLE RULE_GROUP_STORAGE(GROUP_ID VARCHAR(64) PRIMARY KEY NOT NULL,GROUP_NAME VARCHAR(100),PARENT_ID VARCHAR(100),DESCRIPTION CLOB,LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"

  override def TABLE_NAME: String = "RULE_GROUP_STORAGE"

  override val SELECT_ALL: String = "SELECT * FROM RULE_GROUP_STORAGE"
  private val SELECT_BY_GROUP_ID: String = "SELECT * FROM RULE_GROUP_STORAGE WHERE GROUP_ID=?"
  override val INSERT: String = "INSERT INTO RULE_GROUP_STORAGE(GROUP_NAME,PARENT_ID,DESCRIPTION,GROUP_ID) VALUES(?,?,?,?)"
  override val UPDATE: String = "UPDATE RULE_GROUP_STORAGE SET GROUP_NAME = ?,PARENT_ID = ? ,DESCRIPTION = ? ,LAST_TIME=CURRENT_TIMESTAMP WHERE GROUP_ID=?"
  private val DELETE_SQL: String = "DELETE FROM RULE_GROUP_STORAGE WHERE GROUP_ID=? OR PARENT_ID = ?"

  init

  override def get(groupId: String): Option[RuleGroup] = {
    one(SELECT_BY_GROUP_ID)(Array(groupId))
  }

  def getAll(): List[RuleGroup] = {
    getList(SELECT_ALL)()
  }

  override def set(group: RuleGroup): Boolean = {
    if (exist(SELECT_BY_GROUP_ID)(Array(group.id)))
      execute(UPDATE)(Array(group.name, group.parent match {
        case None =>
          ""
        case Some(a) =>
          a
      }, group.description, group.id))
    else
      execute(INSERT)(Array(group.id, group.name, group.parent match {
        case None =>
          ""
        case Some(a) =>
          a
      }, group.description))
  }

  override def delete(groupId: String): Boolean = {
    execute(DELETE_SQL)(Array(groupId, groupId))
  }

  override def entry(set: ResultSet): RuleGroup = {
    val parent = {
      set.getString("PARENT_ID") match {
        case "" =>
          None
        case null =>
          None
        case obj =>
          Some(obj)
      }
    }

    RuleGroup(set.getString("GROUP_ID"), set.getString("GROUP_NAME"), parent, set.getString("DESCRIPTION"), None, None)
  }

  /*
  * id 是必选字段 可以为空
  *   并且id应该为最后一个字段
  *
  * */
  override def from(entity: RuleGroup): Array[Any] = Array(entity.name, entity.parent match {
    case None =>
      ""
    case Some(a) =>
      a
  }, entity.description, entity.id)

  override def where(entity: RuleGroup): String = {
    ""
  }

  private def getList(sql: String)(array: Array[Any] = Array()): List[RuleGroup] = {
    var lcs = List[RuleGroup]()
    val cs = query(sql)(array)
    while (cs.hasNext) {
      lcs = cs.next() :: lcs
    }
    lcs
  }
}
