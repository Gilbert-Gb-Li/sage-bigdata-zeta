package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Mapper


/**
  * Created by zhhuiyan on 2015/3/12.
  */
class DataSourceStore extends DBStore[DataSourceWrapper, String] with Mapper {


  //protected var configs: Map[String, DataSourceWrapper] = Map()

  override val TABLE_NAME = "DATASOURCE"

  val CREATE_TABLE: String =
    s"""CREATE TABLE $TABLE_NAME(
       |ID VARCHAR(64) PRIMARY KEY NOT NULL,
       |NAME VARCHAR(64),
       |DATA CLOB,
       |COLLECTOR VARCHAR(64),
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin

  override val SELECT_BY_ID = s"SELECT * FROM $TABLE_NAME WHERE ID=?"
  override val INSERT: String = s"INSERT INTO $TABLE_NAME(ID, NAME, DATA, COLLECTOR) VALUES(?,?,?,?)"
  override val UPDATE = s"UPDATE $TABLE_NAME SET NAME=?,  DATA=?,COLLECTOR=?, LAST_TIME=? WHERE ID=?"
  override val DELETE = s"DELETE FROM $TABLE_NAME WHERE ID=?"

  override def init: Boolean = {
    if (exist) {
      /*update for 2016 11 16 */
      /*val data = all()
      if (data.exists(_.properties.isEmpty)) {
        if (execute(s"DROP TABLE $TABLE_NAME")()) {
          execute(CREATE_TABLE)()
          data.foreach(set)
        }

      }*/
      true
    }
    else
      execute(CREATE_TABLE)()
  }

  init

  override def from(entity: DataSourceWrapper): Array[Any] =
    Array(entity.name, entity.data, entity.lasttime, entity.createtime, entity.id)


  override def where(entity: DataSourceWrapper): String = {


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

  override def entry(set: ResultSet): DataSourceWrapper = {
    DataSourceWrapper(set.getString("ID"),
      set.getString("NAME"),
      mapper.readValue[DataSource](set.getString("DATA")),
      Option(set.getString("COLLECTOR")),
      Option(set.getTimestamp("LAST_TIME")),
      Option(set.getTimestamp("CREATE_TIME")),
      set.getInt("IS_SAMPLE"))
  }


  protected def getList(sql: String)(array: Array[Any] = Array()): List[DataSourceWrapper] = {
    var lcs = List[DataSourceWrapper]()
    val cs = query(sql)(array)
    while (cs.hasNext) {
      lcs = cs.next() :: lcs
    }
    lcs
  }

  override def all(): List[DataSourceWrapper] = {
    query(SELECT_ALL)(Array()).toList

  }

  def byType(_type: DataSourceType.Type): List[DataSourceWrapper] = {
    val list = all()

    _type match {
      case DataSourceType.Channel =>
        list.filter(_.data.isInstanceOf[Channel])
      case DataSourceType.Table =>
        list.filter(wrapper => {
          wrapper.data.isInstanceOf[Channel] && wrapper.data.name.contains("table")
        })
      case DataSourceType.NotTable =>
        list.filter(wrapper => {
          wrapper.data.isInstanceOf[Channel] && !wrapper.data.name.contains("table")
        })
      case _ =>
        list.filter(!_.data.isInstanceOf[Channel])
    }
  }


  override def set(dataSource: DataSourceWrapper): Boolean = {
    val data = mapper.writeValueAsString(dataSource.data)
    // val properties = mapper.writeValueAsString(dataSource.properties)
    dataSource.id match {
      case null =>
        execute(INSERT)(Array(UUID.randomUUID().toString, dataSource.name,
          data, dataSource.collector.orNull))
      case id =>
        if (exist(SELECT_BY_ID)(Array(id)))
          execute(UPDATE)(Array(dataSource.name,
            data, dataSource.collector.orNull, new Date(), id))
        else execute(INSERT)(Array(id, dataSource.name,
          data, dataSource.collector.orNull))

    }
  }

  def withChannel(id: String): List[DataSourceWrapper] = {
    all().filter(p = dataSource => {
      dataSource.data match {
        case channel: TupleChannel =>
          channel.first.id.contains(id) || channel.second.id.contains(id)
        case channel: SingleTableChannel =>
          channel.channel.id.contains(id)
        case _ => false
      }
    })
  }
}
