package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Mapper

/**
  * Created by evan on 17-8-7.
  */
class ModelingStore extends DBStore[ModelingWrapper, String] with Mapper {
  override protected def TABLE_NAME: String = "MODELING"

  val CREATE_TABLE: String =
    s"""CREATE TABLE $TABLE_NAME(
       |ID VARCHAR(64) PRIMARY KEY NOT NULL,
       |NAME VARCHAR(64),
       |TYPE VARCHAR(100),
       |DATA CLOB,
       |STATUS VARCHAR(64),
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin

  override protected def INSERT: String = s"INSERT INTO $TABLE_NAME(NAME, TYPE, DATA, STATUS, ID) VALUES (?,?,?,?,?)"

  override protected def UPDATE: String = s"UPDATE $TABLE_NAME SET NAME=?, TYPE=?, DATA=?, STATUS=?, LAST_TIME=? WHERE ID=?"

  override val SELECT_BY_ID: String = s"SELECT * FROM $TABLE_NAME WHERE ID=?"
  override val DELETE = s"DELETE FROM $TABLE_NAME WHERE ID=?"
  protected val SELECT_BY_TYPE = s"SELECT * FROM $TABLE_NAME WHERE TYPE=?"

  protected var configs: Map[String, ModelingWrapper] = Map()

  override def init: Boolean = {
    if (exist) {
      configs = getList(SELECT_ALL)().map(conf => (conf.id.get, conf)).toMap
      true
    }
    else
      execute(CREATE_TABLE)()
  }

  init

  protected def getList(sql: String)(array: Array[Any] = Array()): List[ModelingWrapper] = {
    var lks = List[ModelingWrapper]()
    val ks = query(sql)(array)
    while (ks.hasNext) {
      lks = ks.next() :: lks
    }
    lks
  }

  override protected def entry(set: ResultSet): ModelingWrapper =
    mapper.readValue[ModelingWrapper](set.getString("DATA"))
      .copy(id = Some(set.getString("ID")))
      .copy(isSample = set.getInt("IS_SAMPLE"))


  override protected def from(entity: ModelingWrapper): Array[Any] = Array(
    entity.name,
    entity.`type`.map(_.`type`).orNull,
    mapper.writeValueAsString(entity),
    entity.status.toString,
    entity.lasttime,
    entity.createtime,
    entity.id
  )

  override protected def where(entity: ModelingWrapper): String = {

    val name: String = entity.name match {
      case null =>
        ""
      case n =>
        s"name like '%$n%'"
    }

    if (name == "") {
      ""
    } else {
      s" where $name"
    }

  }

  override def all(): List[ModelingWrapper] = {
    var lks=List[ModelingWrapper]()
    val ks= query(SELECT_ALL)(Array())
    while(ks.hasNext){
      lks=ks.next()::lks
    }
    lks
  }

  override def set(handle: ModelingWrapper): Boolean = {
    val data = mapper.writeValueAsString(handle)
    handle.id match {
      case Some(id) =>
        if (exist(SELECT_BY_ID)(Array(id)))
          execute(UPDATE)(Array(handle.name, handle.`type`.get.`type`.toString, data, handle.status.toString, new Date(), id))
        else
          execute(INSERT)(Array(handle.name, handle.`type`.get.`type`.toString, data, handle.status.toString, id))
      case None =>
        execute(INSERT)(Array(handle.name, handle.`type`.get.`type`.toString, data, handle.status.toString, UUID.randomUUID().toString))
    }
  }

  def byDataSource(id: String): List[ModelingWrapper] = {
    init
    configs.values.filter(conf => id.equals(conf.channel)).toList
  }

  def byAnalyzer(id: String): List[ModelingWrapper] = {
    init
    configs.values.filter(conf => conf.analyzer.contains(id)).toList
  }

  def byWrite(id: String): List[ModelingWrapper] = {
    init
    configs.values.filter(conf => conf.sinks.contains(id)).toList
  }

  def byChannel(id: String): List[ModelingWrapper] = {
    init
    configs.values.filter(conf => id.equals(conf.channel)).toList
  }

  def queryByType(`type`: AnalyzerType.Type): List[ModelingWrapper] = {
    val array: Array[Any] = Array(`type`.toString)
    getList(SELECT_BY_TYPE)(array)
  }


  def queryModelingTableList(): List[String] = {
    var lks = List[String]()
    //derby select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME like '%KNOW%'
    //val sql = "SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE = 'T' AND TABLENAME LIKE 'KNOWLEDGE_%@%'"
    val sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME LIKE 'KNOWLEDGE_%@%'"
    val stmt = connection.prepareStatement(sql)
    val set = stmt.executeQuery()
    while (set.next()) {
      lks = set.getString(1) :: lks
    }
    if (set != null && !set.isClosed)
      set.close()
    if (stmt != null && !stmt.isClosed)
      stmt.close()
    connection.commit()
    lks
  }

  override def delete(id: String): Boolean = {
    //删除离线建模表中的记录的同时，删除对应的知识库表,先删除知识库表
    var lks = List[String]()
    val tableName = "KNOWLEDGE_" + id.replace("-", "_").toUpperCase()
   //derby val sql = "SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE = 'T' AND TABLENAME LIKE '" + tableName + "%@%'"
   val sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME LIKE '" + tableName + "%@%'"
    val stmt = connection.prepareStatement(sql)
    val set = stmt.executeQuery()
    while (set.next()) {
      val sql = """drop table """"+set.getString(1)+"""""""
      lks = sql :: lks
    }
    if (set != null && !set.isClosed)
      set.close()
    if (stmt != null && !stmt.isClosed)
      stmt.close()
    connection.commit()
    if (lks.nonEmpty) {
      val rs = lks.map(sql=>execute(sql)())
      if (rs.exists(r => !r))
        false
      else
        execute(DELETE)(Array(id))
    } else
      execute(DELETE)(Array(id))
  }
}