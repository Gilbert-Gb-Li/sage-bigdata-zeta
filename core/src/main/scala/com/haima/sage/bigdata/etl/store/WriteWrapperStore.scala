package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model.{SwitchWriter, WriteWrapper, Writer}
import com.haima.sage.bigdata.etl.utils.Mapper

/**
  * Created: 2016-05-26 11:01.
  * Author:zhhuiyan
  * Created: 2016-05-26 11:01.
  *
  *
  */
class WriteWrapperStore extends DBStore[WriteWrapper, String] with Mapper {

  override val TABLE_NAME = "WRITER"
  val CREATE_TABLE =
    s"""CREATE TABLE $TABLE_NAME(
       |ID VARCHAR(64) PRIMARY KEY NOT NULL,
       |NAME VARCHAR(64),
       |WRITERTYPE VARCHAR(20),
       |DATA CLOB,
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin
  override val INSERT = s"INSERT INTO $TABLE_NAME( NAME, WRITERTYPE, DATA, ID) VALUES (?, ?, ?, ?)"
  override val UPDATE = s"UPDATE $TABLE_NAME SET NAME=?, WRITERTYPE=?, DATA=?, LAST_TIME=? WHERE ID=?"
  override val DELETE = s"DELETE FROM $TABLE_NAME WHERE id=?"

  val QUERY_COUNT_BY_NAME_AND_ID_SQL = s"SELECT COUNT(*) FROM $TABLE_NAME WHERE NAME=? AND ID!=?"
  val QUERY_COUNT_BY_NAME_SQL = s"SELECT COUNT(*) FROM $TABLE_NAME WHERE NAME=?"

  init

  override def entry(set: ResultSet): WriteWrapper = {
    //    val writer = JsonMethods.parse(resultSet.getString("data")).extract[Writer]
    val data = mapper.readValue[Writer](set.getString("DATA"))


    WriteWrapper(
      Some(set.getString("ID")),
      set.getString("NAME"),
      Some(set.getString("WRITERTYPE")),
      data match {
        case switchWriter: SwitchWriter =>
          switchWriter.copy(writers = switchWriter.writers.map {
            case (v, w) =>
              get(w.id) match {
                case Some(x) =>
                  (v, x.data.setId(w.id))
                case None =>
                  logger.error(s"not found sub write[${w.id}]")
                  (v, w)
              }

          }, default = get(switchWriter.default.id) match {
            case Some(x) =>
              x.data.setId(switchWriter.default.id)
            case None =>
              logger.error(s"not found default write[${switchWriter.default.id}]")
              switchWriter.default
          })
        case t =>
          t
      },
      Some(new Date(set.getTimestamp("LAST_TIME").getTime)),
      Some(new Date(set.getTimestamp("CREATE_TIME").getTime)),
      set.getInt("IS_SAMPLE")
    )
  }

  /*
  * id 是必选字段 可以为空
  *   并且id应该为最后一个字段
  *
  * */
  override def from(entity: WriteWrapper): Array[Any] = entity.data match {

    case null => Array()
    case w => Array(entity.id.getOrElse(UUID.randomUUID().toString), entity.name, w.name, mapper.writeValueAsString(entity), entity.lasttime, entity.createtime)
  }

  override def where(entity: WriteWrapper): String = {

    println(entity)

    val _type = entity.writeType match {
      case Some(name) if name != null && name.trim != "" =>
        s" and writerType='$name'"
      case _ =>
        ""
    }
    s" where name like '%${
      entity.name match {
        case name if name == null || name.trim == "" =>
          ""
        case name =>
          name
      }
    }%' ${_type}"
  }

  /*override def queryByPagination(start: Int, limit: Int, orderBy: Option[String],
                                 order: Option[String], searchO: Option[String]) = {
    val sqlCountParams = ArrayBuffer[Any]()
    val sqlCount = new SQL {
      {
        SELECT("COUNT(id)")
        FROM(TABLE_NAME)
        for (search <- searchO) {
          val json = JSON.parseObject(search)
          val name = json.getString("name")
          if (StringUtils.isNotBlank(name)) {
            WHERE("name LIKE ?")
            sqlCountParams += '%' + name + '%'
          }
          val writerType = json.getString("writerType")
          if (StringUtils.isNotBlank(writerType)) {
            WHERE("writerType=?")
            sqlCountParams += writerType
          }
        }
      }
    }.toString
    val total = count(sqlCount)(sqlCountParams.toArray)

    val sqlParams = ArrayBuffer[Any]()
    val sql = new SQL() {
      {
        SELECT("*")
        FROM(TABLE_NAME)
        for (search <- searchO) {
          val json = JSON.parseObject(search)
          val name = json.getString("name")
          if (StringUtils.isNotBlank(name)) {
            WHERE("name like ?")
            sqlParams += '%' + name + '%'
          }
          val writerType = json.getString("writerType")
          if (StringUtils.isNotBlank(writerType)) {
            WHERE("writerType=?")
            sqlParams += writerType
          }
        }
        for (ob <- orderBy) {
          order match {
            case Some(o) => ORDER_BY(ob + " " + o)
            case None => ORDER_BY(ob)
          }
        }
        OFFSET()
        FETCH()
        sqlParams ++= Array(start, limit)
      }
    }.toString
    Pagination[WriteWrapper](start, limit, total, result = query(sql)(sqlParams.toArray).toList)
  }*/

  override def set(writer: WriteWrapper) = {
    writer.data match {
      case null => false
      case w => {
        val data = mapper.writeValueAsString(w)
        writer.id match {
          case Some(id) => {
            if (exist(SELECT_BY_ID)(Array(id)))
              execute(UPDATE)(Array(writer.name, w.name, data, new Date(), id))
            else
              execute(INSERT)(Array(writer.name, w.name, data, id))
          }
          case None => execute(INSERT)(Array(writer.name, w.name, data, UUID.randomUUID().toString))
        }
      }

    }
  }

  def queryCountByName(writeWrapper: WriteWrapper): Int = {
    writeWrapper.id match {
      case Some(id) => count(QUERY_COUNT_BY_NAME_AND_ID_SQL)(Array(writeWrapper.name, id))
      case None => count(QUERY_COUNT_BY_NAME_SQL)(Array(writeWrapper.name))
    }
  }
}