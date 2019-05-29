package com.haima.sage.bigdata.etl.store.rules

import java.sql.ResultSet

import com.haima.sage.bigdata.etl.codec._
import com.haima.sage.bigdata.etl.common.model.filter.Rule
import com.haima.sage.bigdata.etl.common.model.{Parser, RuleUnit}
import com.haima.sage.bigdata.etl.store.DBStore
import com.haima.sage.bigdata.etl.utils.Mapper
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by zhhuiyan on 2015/5/28.
  */
class DBRuleUnitStore extends RuleUnitStore with DBStore[RuleUnit, String] with Mapper {
  override val logger: Logger = LoggerFactory.getLogger(classOf[DBRuleUnitStore])
  val CREATE_TABLE: String = "CREATE TABLE RULE_STORAGE(RULE_ID VARCHAR(64) PRIMARY KEY NOT NULL,RULE_NAME VARCHAR(100),GROUP_ID VARCHAR(64),PARSER CLOB,CODEC CLOB,RULE_VERSION VARCHAR(100),DESCRIPTION CLOB,LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"

  override def TABLE_NAME: String = "RULE_STORAGE"

  override val SELECT_ALL: String = "SELECT * FROM RULE_STORAGE"
  override val SELECT_BY_ID: String = "SELECT * FROM RULE_STORAGE WHERE RULE_ID=?"
  private val SELECT_BY_GROUP_ID: String = "SELECT * FROM RULE_STORAGE WHERE GROUP_ID=?"
  override val INSERT: String = "INSERT INTO RULE_STORAGE(RULE_NAME,GROUP_ID,PARSER,CODEC,RULE_VERSION,DESCRIPTION,RULE_ID) VALUES(?,?,?,?,?,?,?)"
  override val UPDATE: String = "UPDATE RULE_STORAGE SET RULE_NAME = ?,GROUP_ID = ?,PARSER = ?,CODEC = ? ,RULE_VERSION = ? ,DESCRIPTION = ? ,LAST_TIME=CURRENT_TIMESTAMP WHERE RULE_ID=?"
  override val DELETE: String = "DELETE FROM RULE_STORAGE WHERE RULE_ID=?"
  private val DELETE_BY_GROUP_ID: String = "DELETE FROM RULE_STORAGE WHERE GROUP_ID=?"

  init

  override def get(ruleId: String): Option[RuleUnit] = {
    one(SELECT_BY_ID)(Array(ruleId))
  }


  def getByGroup(groupId: String): List[RuleUnit] = {
    getList(SELECT_BY_GROUP_ID)(Array(groupId))
  }

  override def set(rule: RuleUnit): Boolean = {

    if (exist(SELECT_BY_ID)(Array(rule.id)))
      execute(UPDATE)(from(rule))
    else
      execute(INSERT)(from(rule))
  }

  override def delete(ruleId: String): Boolean = {
    execute(DELETE)(Array(ruleId))
  }

  def deleteByGroup(groupId: String): Boolean = {
    execute(DELETE_BY_GROUP_ID)(Array(groupId))
  }

  override def entry(set: ResultSet): RuleUnit = {
    val codec =mapper.readValue[Option[Codec]]( set.getString("CODEC"))
    val parser = mapper.readValue[Parser[Rule]](set.getString("CODEC"))
    RuleUnit(set.getString("RULE_ID"), set.getString("RULE_NAME"), set.getString("GROUP_ID"), parser, codec, set.getString("RULE_VERSION"), set.getString("DESCRIPTION"))
  }

  /*
  * id 是必选字段 可以为空
  *   并且id应该为最后一个字段
  *
  * */
  override def from(entity: RuleUnit): Array[Any] = {

    Array(entity.name, entity.groupId,mapper.writeValueAsString(entity.parser), mapper.writeValueAsString( entity.codec), entity.version, entity.description, entity.id)
  }

  override def where(entity: RuleUnit): String = {
    ""
  }


  private def getList(sql: String)(array: Array[Any] = Array()): List[RuleUnit] = {
    var lcs = List[RuleUnit]()
    val cs = query(sql)(array)
    while (cs.hasNext) {
      lcs = cs.next() :: lcs
    }
    lcs
  }
}
