package com.haima.sage.bigdata.etl.store.position

import java.io.IOException
import java.sql.ResultSet

import com.haima.sage.bigdata.etl.common.model.ReadPosition
import com.haima.sage.bigdata.etl.store.DBStore

/**
  * Created by zhhuiyan on 15/3/10.
  */
class DBReadPositionStore extends ReadPositionStore with DBStore[ReadPosition, String] {

  override val TABLE_NAME: String = "READ_POSITION"

  val CREATE_TABLE: String = s"CREATE TABLE $TABLE_NAME (PATH VARCHAR(500) PRIMARY KEY NOT NULL, POSITION numeric(30), RECORDS numeric(30) , FINISHED SMALLINT,MODIFIED numeric(30))"
  val SELECT: String = s"SELECT POSITION,RECORDS,FINISHED,MODIFIED,PATH FROM $TABLE_NAME WHERE PATH=?"
  val SELECT_FULL: String = s"SELECT POSITION,RECORDS,FINISHED,MODIFIED,PATH FROM $TABLE_NAME WHERE PATH like ?"
  val DELETE_STATEMENT: String = s"DELETE FROM $TABLE_NAME WHERE PATH=?"
  val FUZZY_DELETE: String = s"DELETE FROM $TABLE_NAME WHERE PATH LIKE ?"
  override val UPDATE: String = s"UPDATE $TABLE_NAME SET POSITION = ?, RECORDS = ?, FINISHED= ? ,MODIFIED= ?  WHERE PATH =?"
  override val INSERT: String = s"INSERT INTO $TABLE_NAME(POSITION, RECORDS,FINISHED,MODIFIED,PATH) VALUES(?,?,?,?,?)"
  override val CHECK_TABLE: String = s"UPDATE $TABLE_NAME SET PATH='1' WHERE 1=3"

  init

  override def close() {
    //flush()
    super.close()
  }

  override def set(position: ReadPosition): Boolean = {

    try {
      if (position != null) {
        synchronized {
          get(position.path) match {
            case Some(p) =>
              update(UPDATE)(Array(position.position, position.records, position.finished, position.modified, position.path))
            case _ =>
              try {
                execute(INSERT)(Array(position.position, position.records, position.finished, position.modified, position.path))
              } catch {
                case e: Exception =>
                  e.printStackTrace()
              }

          }
        }

      }
    }
    catch {
      case e: IOException =>
        logger.error("read_position store error,when flush to file:{}", e)
        return false
    }
    true
  }

  def remove(path: String): Boolean = {
    try {
      logger.info(s"delete path = $path")
      execute(DELETE_STATEMENT)(Array(path))
    }
    catch {
      case e: IOException =>
        logger.error(s"read_position store error,when delete $path:{}", e)
        false
    }
  }

  override def fuzzyRemove(path: String): Boolean = {
    try {
      logger.info(s"fuzzy delete path = $path")
      execute(FUZZY_DELETE)(Array(path))
    }
    catch {
      case e: IOException =>
        logger.error(s"read_position store error,when fuzzy delete $path:{}", e)
        false
    }
  }

  override def get(path: String): Option[ReadPosition] = {
    one(SELECT)(Array(path))

  }

  override def list(path: String): List[ReadPosition] = {
    query(SELECT_FULL)(Array(s"$path%")).toList
  }

  @throws(classOf[IOException])
  def flush() {

  }


  override def entry(set: ResultSet): ReadPosition = {
    val pos: Long = set.getLong("POSITION")
    val records: Long = set.getLong("RECORDS")
    val finished: Boolean = set.getBoolean("FINISHED")
    val modified: Long = set.getLong("MODIFIED")
    val path: String = set.getString("PATH")
    val position: ReadPosition = ReadPosition(path, records, pos, finished, modified)
    position
  }

  /*
  * id 是必选字段 可以为空
  *   并且id应该为最后一个字段
  *
  * */
  override def from(entity: ReadPosition): Array[Any] = Array(entity.position, entity.records, entity.finished, entity.modified, entity.path)

  override def where(entity: ReadPosition): String = {
    ""
  }
}
