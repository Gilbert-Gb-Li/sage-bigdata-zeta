package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, _}


/**
  * Created by zhhuiyan on 2015/3/12.
  */
class DBAnalyzerStore extends AnalyzerStore with DBStore[AnalyzerWrapper, String] {


  //protected var configs: Map[String, HandleWrapper] = Map()

  override val TABLE_NAME = "ANALYZER"

  protected val CREATE_TABLE: String =
    s"""CREATE TABLE ${TABLE_NAME}(
       |ID VARCHAR(64) PRIMARY KEY NOT NULL,
       |NAME VARCHAR(64),
       |DATA CLOB,
       |CHANNEL VARCHAR(64),
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin
  override val INSERT = s"INSERT INTO $TABLE_NAME( NAME, DATA,CHANNEL, ID) VALUES (?,?,?,?)"
  override val UPDATE = s"UPDATE $TABLE_NAME SET NAME=?, DATA=?,CHANNEL=?, LAST_TIME=? WHERE ID=?"
  override val DELETE = s"DELETE FROM $TABLE_NAME WHERE ID=?"

  val QUERY_COUNT_BY_NAME_AND_ID_SQL = s"SELECT COUNT(*) FROM $TABLE_NAME WHERE NAME=? AND ID!=?"
  val QUERY_COUNT_BY_NAME_SQL = s"SELECT COUNT(*) FROM $TABLE_NAME WHERE NAME=?"
  val SELECT_BY_TYPE = s"SELECT * FROM $TABLE_NAME WHERE TYPE=?"

  init

  override def from(entity: AnalyzerWrapper): Array[Any] =
    Array(entity.name,
      mapper.writeValueAsString(entity.data),
      entity.channel.orNull,
      entity.lasttime,
      entity.createtime,
      entity.id)


  override def where(entity: AnalyzerWrapper): String = {


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

  override def entry(set: ResultSet): AnalyzerWrapper = {
    val data = mapper.readValue[Analyzer](set.getString("DATA"))


    AnalyzerWrapper(
      Option(set.getString("ID")),
      set.getString("NAME"),
      Option(data),
      Option(set.getString("CHANNEL")),
      Option(set.getTimestamp("LAST_TIME")),
      Option(set.getTimestamp("CREATE_TIME")),
      set.getInt("IS_SAMPLE")
    )
  }


  protected def getList(sql: String)(array: Array[Any] = Array()): List[AnalyzerWrapper] = {
    var lcs = List[AnalyzerWrapper]()
    val cs = query(sql)(array)
    while (cs.hasNext) {
      lcs = cs.next() :: lcs
    }
    lcs
  }

  override def all(): List[AnalyzerWrapper] = {
    query(SELECT_ALL)(Array()).toList

  }

  override def set(handle: AnalyzerWrapper): Boolean = {
    val data = mapper.writeValueAsString(handle.data)
    logger.debug(s"save analyzer is ${data}")
    handle.id match {
      case Some(id) =>
        if (exist(SELECT_BY_ID)(Array(id)))
          execute(UPDATE)(Array(handle.name, data, handle.channel.orNull, new Date(), id))
        else
          execute(INSERT)(Array(handle.name, data, handle.channel.orNull, id))
      case None =>
        execute(INSERT)(Array(handle.name, data, handle.channel.orNull, UUID.randomUUID().toString))
    }
  }

  def queryByType(`type`: AnalyzerType.Type): List[AnalyzerWrapper] = {
    val array: Array[Any] = Array(`type`.toString)
    getList(SELECT_BY_TYPE)(array)
  }
}
