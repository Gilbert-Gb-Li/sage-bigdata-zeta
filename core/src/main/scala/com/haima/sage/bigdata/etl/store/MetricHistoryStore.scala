package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.concurrent.locks.ReentrantLock

import com.haima.sage.bigdata.etl.common.model.MetricHistory
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by evan on 17-10-18.
  */
class MetricHistoryStore extends DBStore[MetricHistory, String] {
  override val logger: Logger = LoggerFactory.getLogger(classOf[MetricHistoryStore])

  override protected def TABLE_NAME: String = "METRIC_HISTORY"

  protected val CREATE_TABLE: String =
    s""" CREATE TABLE $TABLE_NAME(
       |METRIC_ID VARCHAR(512),
       |CONFIG_ID VARCHAR(64),
       |HISTORY_TYPE VARCHAR(64),
       |COUNT BIGINT,
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)""".stripMargin

  override protected def INSERT: String = s"INSERT INTO $TABLE_NAME(METRIC_ID, CONFIG_ID,HISTORY_TYPE,COUNT) VALUES(?,?,?,?)"

  override protected def UPDATE: String = s"UPDATE $TABLE_NAME SET COUNT = ? AND LAST_TIME = ? WHERE CONFIG_ID = ? AND HISTORY_TYPE = ?"

  override def CHECK_TABLE: String = s"UPDATE $TABLE_NAME SET METRIC_ID='1' WHERE 1=3"

  private val SELECT_BY_CONFIG = s"SELECT * FROM $TABLE_NAME WHERE CONFIG_ID = ?"
  private val DELETE_BY_CONFIG = s"DELETE FROM $TABLE_NAME WHERE CONFIG_ID = ?"

  override def init: Boolean = {
    val lock = new ReentrantLock()
    lock.lock()
    val flag = if (exist) {
      true
    } else {
      execute(CREATE_TABLE)()
      execute(s"CREATE INDEX METRIC_HISTORY_TYPE_SELECT_LAST_TIME_DESC ON $TABLE_NAME(METRIC_ID,CONFIG_ID,HISTORY_TYPE,LAST_TIME DESC ) ")()

    }
    lock.unlock()
    flag
  }

  init

  override def set(metricHistory: MetricHistory): Boolean = {
    execute(INSERT)(Array(metricHistory.metricId, metricHistory.configId, metricHistory.historyType, metricHistory.count))
  }

  override protected def entry(set: ResultSet): MetricHistory =
    MetricHistory(set.getString("METRIC_ID"),
      set.getString("CONFIG_ID"),
      set.getString("HISTORY_TYPE"),
      set.getLong("COUNT"),
      Option(set.getTimestamp("LAST_TIME")),
      Option(set.getTimestamp("CREATE_TIME")))

  override protected def from(entity: MetricHistory): Array[Any] = Array(entity.metricId, entity.configId, entity.historyType, entity.count, entity.lasttime, entity.createtime)

  override protected def where(entity: MetricHistory): String = ""

  def getByConfigId(configId: String): List[MetricHistory] = {
    query(SELECT_BY_CONFIG)(Array(configId)).toList
  }

  def deleteByConfigId(configId: String): Boolean = {
    if (execute(DELETE_BY_CONFIG)(Array(configId))) true else false
  }
}
