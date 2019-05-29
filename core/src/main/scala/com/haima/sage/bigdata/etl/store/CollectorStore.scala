package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.{Collector, Collectors, Status}

/**
  * Created by zhhuiyan on 2015/3/12.
  */
class CollectorStore extends DBStore[Collector, String] {
  // cache some info for master system
  private lazy val collectors = scala.collection.mutable.HashMap[String, Collector]()

  override def TABLE_NAME: String = "COLLECTOR"

  val CREATE_TABLE: String =
    s"""CREATE TABLE $TABLE_NAME(
       |ID VARCHAR(64) PRIMARY KEY NOT NULL,
       |NAME VARCHAR(64),
       |IP VARCHAR(15),
       |PORT INT,
       |ADDRESS VARCHAR(200),
       |STATUS VARCHAR(64),
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)""".stripMargin

  private val SELECT_ONLY: String = s"SELECT * FROM $TABLE_NAME WHERE ID = ? OR (IP = ? AND PORT = ?) FETCH FIRST 1 ROWS ONLY"
  override val INSERT: String = s"INSERT INTO $TABLE_NAME(IP, PORT, NAME,ADDRESS, STATUS, ID) VALUES(?,?,?,?,?,?)"
  override val UPDATE: String = s"UPDATE $TABLE_NAME SET  IP = ?, PORT = ?,NAME = ?,ADDRESS=?,STATUS=?, LAST_TIME=? WHERE ID=?"

  val QUERY_NAME_BY_ID_SQL = s"SELECT * FROM $TABLE_NAME WHERE ID=?"
  val QUERY_BY_RELAY_SQL = s"SELECT * FROM $TABLE_NAME WHERE ADDRESS=?"

  override val SELECT_COUNT = s"SELECT COUNT(ID) FROM $TABLE_NAME"


  override def init: Boolean = {


    if (exist) {
      if (metadata().keys.exists(_ == "STATUS")) {
        var servers = List[Collector]()
        val si = query(SELECT_ALL)()
        while (si.hasNext) {
          servers = si.next() :: servers
        }
        servers.foreach(server => collectors + (server.id -> server))
      } else {
        execute(s"drop table if exists $TABLE_NAME")()
        execute(CREATE_TABLE)()
      }


      true
    } else {
      execute(CREATE_TABLE)()
    }
  }

  init

  override def set(collector: Collector): Boolean = {
    if (exist(SELECT_BY_ID)(Array(collector.id)))
      execute(UPDATE)(Array(collector.host, collector.port, collector.system, collector.relayServer.map(_.address()).getOrElse(""), collector.status.map(_.toString).orNull, new Date(), collector.id))
    else {
      if (execute(INSERT)(Array(collector.host, collector.port, collector.system,collector.relayServer.map(_.address()).getOrElse(""), collector.status.map(_.toString).orNull, collector.id))) {
        collectors += (collector.id -> collector)
        true
      } else {
        false
      }

    }
  }

  override def delete(id: String): Boolean = {
    if (execute(DELETE)(Array(id))) {
      collectors -= id
      true
    } else {
      false
    }
  }

  override def get(id: String): Option[Collector] = {
    collectors.get(id) match {
      case Some(server) => Some(server)
      case None =>
        one(SELECT_BY_ID)(Array(id))

    }

  }

  override def entry(set: ResultSet): Collector = {
    Collector(
      set.getString("ID"),
      set.getString("IP"),
      set.getInt("PORT"),
      set.getString("NAME"),
      "worker",
      Collectors.from(set.getString("ADDRESS")),
      Option(Status.withName(set.getString("STATUS"))),
      Option(set.getTimestamp("LAST_TIME")),
      Option(set.getTimestamp("CREATE_TIME"))
    )
  }

  override def from(entity: Collector): Array[Any] = Array(entity.host, entity.port, entity.system, entity.relayServer.map(_.address()).getOrElse(""), entity.status.map(_.toString).orNull, entity.id, new Date())

  override def where(entity: Collector): String = {
    ""
  }

  def get(collector: Collector): Option[Collector] = {
    collectors.get(collector.id) match {
      case Some(server) => Some(server)
      case None =>
        one(SELECT_ONLY)(Array(collector.id, collector.host, collector.port))
    }
  }


  override def all(): List[Collector] = {
    //collectors.values.toList
    query(SELECT_ALL)().toList
  }

  def queryNameById(id: String): Option[Collector] = {
    one(QUERY_NAME_BY_ID_SQL)(Array(id))
  }

  def queryByRelayServer(relay: Collector): List[Collector] = {
    query(QUERY_BY_RELAY_SQL)(Array(relay.address())).toList
  }

  /* override def queryByPagination(start: Int, limit: Int, orderBy: String, order: String)(params: Array[Any] = Array()) = {
     val total = count(SELECT_COUNT)(params)
     Pagination[Collector](start, limit, total, result = query(SELECT_PAGINATION)(params.+:(limit).+:(start)).toList)
   }*/

}
