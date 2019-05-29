package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model.TaskLogInfoWrapper
import com.haima.sage.bigdata.etl.utils.Mapper

/**
  * Created by liyju on 2018/3/21.
  */
class TaskLogInfoStore  extends DBStore[TaskLogInfoWrapper, String] with Mapper {
  override protected def TABLE_NAME: String = "TASK_LOG_INFO"
  protected var timerMsgWrappers:Map[String,TaskLogInfoWrapper]=Map()
  override protected def CREATE_TABLE: String = s"""CREATE TABLE $TABLE_NAME(
                                                   |ID VARCHAR(64) PRIMARY KEY NOT NULL,
                                                   |TASK_ID VARCHAR(64),
                                                   |TASK_TYPE VARCHAR(20),
                                                   |CONFIG_ID VARCHAR(64),
                                                   |ACTION VARCHAR(64),
                                                   |MSG VARCHAR(500),
                                                   |TASK_STATUS VARCHAR(15),
                                                   |CONFIG_STATUS VARCHAR(15),
                                                   |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                   |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                   |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin

  override protected def INSERT: String = s"INSERT INTO $TABLE_NAME(ID, TASK_ID, TASK_TYPE, CONFIG_ID,ACTION,MSG,TASK_STATUS,CONFIG_STATUS) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

  override protected def UPDATE: String = s"UPDATE $TABLE_NAME set TASK_ID=?, TASK_TYPE=?, CONFIG_ID=?, ACTION=?, MSG=?,TASK_STATUS=?,CONFIG_STATUS=? LAST_TIME=? WHERE ID=?"

  protected def SELECT_BY_TIMERID :String= s"SELECT * FROM  $TABLE_NAME where TASK_ID=? "

  override def init:Boolean={
    if(exist){
      timerMsgWrappers=getList(SELECT_ALL)().map(conf => (conf.id, conf)).toMap
      true
    }
    else
      execute(CREATE_TABLE)()
  }
  init

  protected def getList(sql: String)(array: Array[Any] = Array()): List[TaskLogInfoWrapper] ={
    var lks=List[TaskLogInfoWrapper]()
    val ks=query(sql)(array)
    while(ks.hasNext){
      lks=ks.next()::lks
    }
    lks
  }

  override def all(): List[TaskLogInfoWrapper] = {
    query(SELECT_ALL)(Array()).toList
  }

  override protected def entry(set: ResultSet): TaskLogInfoWrapper = {
    TaskLogInfoWrapper(set.getString("ID"),
      set.getString("TASK_ID"),
      Option(set.getString("TASK_TYPE")),
      Option(set.getString("CONFIG_ID")),
      Option(set.getString("ACTION")),
      Option(set.getString("MSG")),
      Option(set.getString("TASK_STATUS")),
      Option(set.getString("CONFIG_STATUS")),
      Option(set.getTimestamp("LAST_TIME")),
      Option(set.getTimestamp("CREATE_TIME")),
      set.getInt("IS_SAMPLE"))
  }

  override protected def from(entity: TaskLogInfoWrapper): Array[Any] = {
    Array(entity.id, entity.taskId, entity.taskType, entity.action, entity.msg,entity.configStatus,entity.taskStatus,entity.lasttime, entity.createtime)
  }

  override protected def where(entity: TaskLogInfoWrapper): String = {
   if(entity.taskId !=null && "".equals(entity.taskId) ){
     s" where task_id like '%${entity.taskId}%'"
   } else
     ""
  }

  override def set(entity: TaskLogInfoWrapper): Boolean = {
    entity.id match{
      case id:String =>
        if(exist(SELECT_BY_ID)(Array(entity.id)))
          execute(UPDATE)(Array(entity.taskId,  entity.taskType.orNull, entity.configId.orNull,
            entity.action.orNull, entity.msg.orNull,entity.taskStatus.orNull,entity.configStatus.orNull,new Date(),id))
        else
          execute(INSERT)(Array(id,entity.taskId,  entity.taskType.orNull, entity.configId.orNull,
            entity.action.orNull, entity.msg.orNull,entity.taskStatus.orNull,entity.configStatus.orNull))
      case _ =>
        execute(INSERT)(Array( UUID.randomUUID().toString,entity.taskId,  entity.taskType.orNull, entity.configId.orNull,
          entity.action.orNull, entity.msg.orNull,entity.taskStatus.orNull,entity.configStatus.orNull))
    }
    init
  }

  def getByTaskId(taskId:String): List[TaskLogInfoWrapper] = getList(SELECT_BY_TIMERID)(Array(taskId))
}
