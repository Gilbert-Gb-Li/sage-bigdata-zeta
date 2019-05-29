package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model.{ Task, TaskWrapper}
import com.haima.sage.bigdata.etl.utils.Mapper

/**
  * Created: 2016-05-26 11:01.
  * Author:zhhuiyan
  * Created: 2016-05-26 11:01.
  *
  *
  */
 class TaskWrapperStore extends  DBStore[TaskWrapper, String] with Mapper {

  override val TABLE_NAME = "TASK_MANAGER"
  val CREATE_TABLE: String = s"""CREATE TABLE $TABLE_NAME(
                               |ID VARCHAR(64) PRIMARY KEY NOT NULL,
                               |NAME VARCHAR(64),
                               |CONFIG VARCHAR(64),
                               |DATA CLOB,
                               |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                               |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                               |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin
  override val INSERT = s"INSERT INTO $TABLE_NAME(NAME, CONFIG, DATA, ID) VALUES (?, ?,?,  ?)"
  override val UPDATE = s"UPDATE $TABLE_NAME SET NAME=?,CONFIG=?,DATA=?, LAST_TIME=? WHERE ID=?"
  override val DELETE = s"DELETE FROM $TABLE_NAME WHERE ID=?"

  val QUERY_COUNT_BY_NAME_AND_ID_SQL = s"SELECT COUNT(*) FROM $TABLE_NAME WHERE NAME=? AND ID!=?"
  val QUERY_COUNT_BY_NAME_SQL = s"SELECT COUNT(*) FROM $TABLE_NAME WHERE NAME=?"

  init


  override def entry(set: ResultSet): TaskWrapper = {
    val data = mapper.readValue[Task](set.getString("DATA"))

    TaskWrapper(
      Some(set.getString("ID")),
      set.getString("NAME"),
      set.getString("CONFIG"),
      data,
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
  override def from(entity: TaskWrapper): Array[Any] = {
    Array(entity.id.getOrElse(UUID.randomUUID().toString), entity.name, mapper.writeValueAsString(entity), entity.lasttime, entity.createtime)
  }

  override def where(entity: TaskWrapper): String = {

   val name =  entity.name match {
      case null =>
        ""
      case n =>
        s"name like '%$n%'"
    }

    val jobType = entity.data match{
      case null=>
        ""
      case data =>
        data.jobType match {
          case Some(job:String) if job!=null && !"".equals(job)=> //"jobType":"channel"
            """data like '%"jobType":""""+s"$job"+""""%'"""
          case _ =>
            ""
        }
    }

    if (name == "" && jobType=="" ) {
      ""
    } else if(name != ""){
      s" where $name" + {if(!"".equals(jobType)) s"and $jobType" else ""}
    }else{
      s" where $jobType "
    }
  }


   def byConf(conf: String): List[TaskWrapper] = {
     init
     val rt = query(SELECT_ALL + " where config=?")(Array(conf)).toList
     rt
  }

  override def set(taskWrapper: TaskWrapper): Boolean = {
    val data = mapper.writeValueAsString(taskWrapper.data)
    taskWrapper.id match {
      case Some(id) if exist(SELECT_BY_ID)(Array(id)) =>
        execute(UPDATE)(Array(taskWrapper.name,taskWrapper.data.configId, data, new Date(), id))
      case Some(id) =>
        execute(INSERT)(Array(taskWrapper.name,taskWrapper.data.configId, data, id))
      case None =>
        execute(INSERT)(Array(taskWrapper.name,taskWrapper.data.configId, data, UUID.randomUUID().toString))
    }
  }

   def countByName(taskWrapper: TaskWrapper): Int = {
     taskWrapper.id match {
      case Some(id) => count(QUERY_COUNT_BY_NAME_AND_ID_SQL)(Array(taskWrapper.name, id))
      case None => count(QUERY_COUNT_BY_NAME_SQL)(Array(taskWrapper.name))
    }
  }
}