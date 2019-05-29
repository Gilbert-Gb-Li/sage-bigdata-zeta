package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model.ConfigWrapper
import com.haima.sage.bigdata.etl.common.model.Status.Status
import com.haima.sage.bigdata.etl.utils.Mapper

/**
  * Created by zhhuiyan on 2015/3/12.
  */
class ConfigStore extends DBStore[ConfigWrapper, String] with Mapper {


  protected var configs: Map[String, ConfigWrapper] = Map()
  override val TABLE_NAME: String = "CONFIG"
  val CREATE_TABLE: String =
    s"""CREATE TABLE $TABLE_NAME(
       |ID VARCHAR(64) PRIMARY KEY NOT NULL,
       |NAME VARCHAR(64),
       |DATA CLOB,
       |STATUS VARCHAR(64),
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin

  override val SELECT_BY_ID: String = s"SELECT * FROM $TABLE_NAME WHERE ID=?"
  override val INSERT: String = s"INSERT INTO $TABLE_NAME(NAME,DATA,ID) VALUES(?,?,?)"
  override val UPDATE: String = s"UPDATE $TABLE_NAME SET NAME=?, DATA=?, LAST_TIME=? WHERE ID=?"
  override val DELETE: String = s"DELETE FROM $TABLE_NAME WHERE ID=?"
  override val CHECK_TABLE: String = s"UPDATE $TABLE_NAME SET ID='1' WHERE 1=3"

  override def init: Boolean = {
    if (exist) {
      configs = query(SELECT_ALL)().map(conf => (conf.id, conf)).toMap
      true
    } else {
      execute(CREATE_TABLE)()
    }

  }

  init

  override def from(entity: ConfigWrapper): Array[Any] = {
    Array(entity.name, mapper.writeValueAsString(entity), entity.status.toString, entity.id)


  }

  override def where(entity: ConfigWrapper): String = {

    val name: String = entity.name match {
      case null =>
        ""
      case n =>
        s"name like '%$n%'"
    }

    if (name == "") {
      ""
    } else {
      s" where $name "
    }
  }

  override def entry(set: ResultSet): ConfigWrapper = {
    mapper.readValue[ConfigWrapper](set.getString("DATA")).
      copy(id = set.getString("ID"),
        isSample = set.getInt("IS_SAMPLE"),
        lasttime = Option(set.getTimestamp("LAST_TIME")),
        createtime = Option(set.getTimestamp("CREATE_TIME"))
      )

    /*ConfigWrapper(set.getString("ID"), set.getString("COLLECTOR"), ),
      set.getString("MONITOR_STATUS"),
      Option(new Date(set.getTimestamp("LAST_TIME").getTime)),
      Option(new Date(set.getTimestamp("CREATE_TIME").getTime)))*/
  }

  def byCollector(id: String): List[ConfigWrapper] = {
    configs.values.filter(conf => conf.collector.contains(id)).toList
  }

  def byWriter(id: String): List[ConfigWrapper] = {
    configs.values.filter(conf => conf.writers.contains(id)).toList
  }

  def byParser(id: String): List[ConfigWrapper] = {
    configs.values.filter(conf => conf.parser.contains(id)).toList
  }

  def byDatasource(id: String): List[ConfigWrapper] = {
    configs.values.filter(conf => conf.datasource.contains(id)).toList
  }

  override def all(): List[ConfigWrapper] = {
    configs.values.toList
  }

  def byStatus(status: Status, collectorId: Option[String]): List[ConfigWrapper] = {
    configs.values.filter(conf =>
      conf.status == status && (collectorId match {
        case Some(collector) =>
          conf.collector.contains(collector)
        case _ =>
          true
      })).toList
  }


  override def delete(id: String): Boolean = {
    if (super.delete(id)) {
      configs =configs- id
      true
    } else {
      false
    }
  }

  override def set(wrapper: ConfigWrapper): Boolean = {
    val flag = if (exist(SELECT_BY_ID)(Array(wrapper.id)))
      execute(UPDATE)(Array(wrapper.name, mapper.writeValueAsString(wrapper), new Date(), wrapper.id))
    else
      execute(INSERT)(Array(wrapper.name, mapper.writeValueAsString(wrapper), if (wrapper.id == null) UUID.randomUUID().toString else wrapper.id))
    get(wrapper.id).foreach(d => configs = configs + (d.id -> d))
    flag
  }
}
