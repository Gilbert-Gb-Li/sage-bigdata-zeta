package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet

import com.haima.sage.bigdata.etl.common.model._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by zhhuiyan on 2015/2/12.
  */
class StatusStore extends DBStore[RunStatus, String] {

  override val logger: Logger = LoggerFactory.getLogger(classOf[StatusStore])
  override val TABLE_NAME: String = "STATUS_INFO"

  val CREATE_TABLE: String = s"CREATE TABLE $TABLE_NAME(ID VARCHAR(100),PATH VARCHAR(2000),TYPE VARCHAR(100),VALUE VARCHAR(100),ERROR_MSG VARCHAR(2000))"


  override val INSERT: String = s"INSERT INTO $TABLE_NAME(ID,PATH,TYPE,VALUE,ERROR_MSG) VALUES(?,?,?,?,?)"

  override val UPDATE: String = s"UPDATE $TABLE_NAME SET VALUE =?,ERROR_MSG=? WHERE ID =? AND PATH =? AND TYPE =?"

  override val CHECK_TABLE: String = s"UPDATE $TABLE_NAME SET ID='1' WHERE 1=3"


  var cache: List[RunStatus] = List()


  init

  override def init: Boolean = {
    val rt = super.init
    cache = all()
    rt
  }

  /*
    * id 是必选字段 可以为空
    *   并且id应该为最后一个字段
    *
    * */
  override def from(entity: RunStatus): Array[Any] = Array(entity.value.toString, entity.path.orNull, entity.model.toString, entity.config)

  override def where(entity: RunStatus): String = {
    ""
  }


  /**
    * 删除状态信息
    *
    * @param id
    */
  def deleteByConfig(id: String): Boolean = {
    cache = cache.filterNot(_.config == id)
    execute("DELETE FROM STATUS_INFO WHERE ID = ?")(Array(id))
  }

  /**
    * 查询状态
    *
    * @param id
    * @return HashMap(ID,List[(path,TYPE,VALUE)])
    */
  def status(id: String): List[RunStatus] = {

    val data = cache.filter(_.config == id)
    data
    /*val start = System.currentTimeMillis()
    logger.info(s"get config[$id] status start")

    val rt = query("SELECT * FROM STATUS_INFO WHERE ID=?")(Array(id)).toList

    logger.info(s"get config[$id] status take ${System.currentTimeMillis() - start} ms")

    rt*/
  }


  /**
    * RUNNING, ERROR, PENDING, STOPPED
    * 只有STOPPED才停止,其他都启动
    *
    * @param id
    */
  def monitorStatus(id: String): Option[Status.Status] = {

    cache.filter(data => data.config == id && data.model == ProcessModel.MONITOR) match {
      case Nil =>
        None
      case head :: _ =>
        Some(head.value)
    }
    /* one("SELECT * FROM STATUS_INFO WHERE ID = ? AND TYPE =? ")(Array(id, ProcessModel.MONITOR.toString)) match {
       case None =>
         None
       case Some(s) =>
         Some(s.value)
     }*/

  }

  /**
    * insert status to db
    *
    * @param status
    * @return
    */
  def save(status: RunStatus): Boolean = {
    cache = cache.filterNot(data => data.config == status.config && data.model == status.model && data.path == status.path).::(status)
    //logger.info("save status"+status)
    status.model match {
      case ProcessModel.MONITOR =>
        if (exist(status)) {
          //如果存在该monitor的状态，并且当前状态是ERROR,要修改成STOP，则ERROR_MSG不更新
          if(status.value==Status.STOPPED && get(status.config).get.value==Status.ERROR)
            execute("UPDATE STATUS_INFO SET VALUE = ? WHERE ID = ? AND TYPE = ?")(Array(status.value.toString,status.config, status.model.toString))
          else
            execute("UPDATE STATUS_INFO SET VALUE = ?, ERROR_MSG = ? WHERE ID = ? AND TYPE = ?")(Array(status.value.toString, status.errorMsg.getOrElse(""), status.config, status.model.toString))
        } else {
          execute("INSERT INTO STATUS_INFO(ID,TYPE,VALUE,ERROR_MSG) VALUES(?,?,?,?)")(Array(status.config, status.model.toString, status.value.toString, status.errorMsg.getOrElse("")))
        }
      case _ =>
        if (exist(status)) {
          execute(UPDATE)(Array(status.value.toString, status.errorMsg.orNull, status.config, status.path.get, status.model.toString))
        } else {
          execute(INSERT)(Array(status.config, status.path.get, status.model.toString, status.value.toString,status.errorMsg.orNull))
        }
    }


  }

  /**
    * select status from db
    *
    * @param status : Status
    * @return
    */
  private def exist(status: RunStatus): Boolean = {
    status.model match {
      case ProcessModel.MONITOR =>
        exist("SELECT VALUE FROM STATUS_INFO WHERE ID = ? AND TYPE = ?")(Array(status.config, status.model.toString))
      case _ =>
        exist("SELECT VALUE FROM STATUS_INFO WHERE ID = ? AND PATH = ? AND TYPE = ?")(Array(status.config, status.path.get, status.model.toString))

    }
  }

  override def entry(set: ResultSet): RunStatus = {
    RunStatus(
      set.getString("ID"),
      Option(set.getString("path")),
      ProcessModel.withName(set.getString("type")),
      Status.withName(set.getString("value")),
      Option(set.getString("ERROR_MSG")))
  }
}
