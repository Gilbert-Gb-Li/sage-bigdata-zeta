package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.concurrent.locks.ReentrantLock

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.MetricPhase.MetricPhase
import com.haima.sage.bigdata.etl.common.model.MetricTable.MetricTable
import com.haima.sage.bigdata.etl.common.model.MetricType.MetricType
import com.haima.sage.bigdata.etl.common.model.{MetricInfo, MetricPhase, MetricType}
import com.haima.sage.bigdata.etl.utils.TimeUtils
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by zhhuiyan on 2015/3/12.
  */
class MetricInfoStore(table: MetricTable) extends DBStore[MetricInfo, String] {
  override val logger: Logger = LoggerFactory.getLogger(classOf[MetricInfoStore])

  override def TABLE_NAME: String = s"$table"

  protected val CREATE_TABLE: String =
    s""" CREATE TABLE $TABLE_NAME(
       |ID VARCHAR(64),
       |METRIC_ID VARCHAR(512),
       |CONFIG_ID VARCHAR(512),
       |METRIC_TYPE VARCHAR(64),
       |METRIC_PHASE VARCHAR(64),
       |METRIC CLOB,
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)""".stripMargin

  private val SELECT_SQL: String = s"SELECT * FROM $TABLE_NAME WHERE CONFIG_ID = ? AND LAST_TIME > ? and LAST_TIME < ? ORDER BY LAST_TIME ASC"
  private val TYPE_SELECT_SQL: String = s"SELECT * FROM $TABLE_NAME WHERE ID = ? AND CONFIG_ID = ? AND METRIC_TYPE = ? AND METRIC_PHASE = ? AND LAST_TIME > ? and LAST_TIME < ? ORDER BY LAST_TIME ASC"
  override val INSERT: String = s"INSERT INTO $TABLE_NAME(ID,METRIC_ID,CONFIG_ID,METRIC_TYPE,METRIC_PHASE,METRIC) VALUES(?,?,?,?,?,?)"
  override val UPDATE: String = s"INSERT INTO $TABLE_NAME(ID,METRIC_ID,CONFIG_ID,METRIC_TYPE,METRIC_PHASE,METRIC) VALUES(?,?,?,?,?,?)"
  private val DELETE_SQL: String = s"DELETE FROM $TABLE_NAME WHERE CONFIG_ID=? AND LAST_TIME >= ? and LAST_TIME < ?"
  private val DELETE_BY_DATE_SQL: String = s"DELETE FROM $TABLE_NAME WHERE LAST_TIME >= ? and LAST_TIME < ?"
  private val DELETE_BEFORE_SQL: String = s"DELETE FROM $TABLE_NAME WHERE LAST_TIME < ?"
  private val SELECT_BY_METRIC_ID: String = s"SELECT * FROM $TABLE_NAME WHERE METRIC_ID = ? ORDER BY LAST_TIME DESC FETCH FIRST 1 ROWS ONLY"
  private val SELECT_METRICS_BY_METRIC_ID: String = s"SELECT * FROM $TABLE_NAME WHERE METRIC_ID = ? AND LAST_TIME > ? and LAST_TIME < ? ORDER BY LAST_TIME ASC"
  private val COUNT: String = s"SELECT COUNT(*) FROM $TABLE_NAME WHERE CONFIG_ID=? AND LAST_TIME >= ? and LAST_TIME < ?"

  override def init: Boolean = {
    val lock = new ReentrantLock()
    lock.lock()
    val flag = if (exist) {
      true
    } else {
      execute(CREATE_TABLE)()
      execute(s"CREATE INDEX ${TABLE_NAME}_TYPE_SELECT_LAST_TIME_DESC ON $TABLE_NAME(ID,METRIC_ID,CONFIG_ID,METRIC_TYPE,LAST_TIME DESC ) ")()
    }
    execute(s"CREATE INDEX IF NOT EXISTS ${TABLE_NAME}_TYPE_QUERY_BY_CONFIG_ID ON $TABLE_NAME(CONFIG_ID,LAST_TIME DESC ) ")()
    lock.unlock()
    flag
  }

  init

  override def set(metricInfo: MetricInfo): Boolean = {
    execute(INSERT)(Array(metricInfo.collectorId, metricInfo.metricId, metricInfo.configId, metricInfo.metricType.toString, metricInfo.metricPhase.toString, mapper.writeValueAsString(metricInfo.metric)))
  }

  def delete(collectorId: String, id: String, from: String, to: String): Boolean = {
    execute(DELETE_SQL)(Array(collectorId, id, from, to))
  }

  def delete(from: String, to: String): Boolean = {
    logger.info(s"delete metrics info from $from to $to")
    execute(DELETE_BY_DATE_SQL)(Array(from, to))
  }

  def deleteBefore(before: String): Boolean = {
    logger.info(s"delete metricsInfo in $TABLE_NAME before $before")
    execute(DELETE_BEFORE_SQL)(Array(before))
  }

  def count(configId: String, from: String, to: String): Int = {
    count(COUNT)(Array(configId, from, to))
  }

  def get(configId: String, metricPhase: Option[MetricPhase], metricType: Option[MetricType], from: Option[String], to: Option[String]): List[MetricInfo] = {
    logger.debug(s"query metric for channel[$configId]")

    var fromVal: String = ""
    var toVal: String = ""
    // toVal 默认为currentDate
    to match {
      case Some(v) => toVal = v
      case None => toVal = TimeUtils.defaultFormat(TimeUtils.currentDate)
    }
    // fromVal 默认为toVal之前的10分钟
    from match {
      case Some(v) => fromVal = v
      case None => fromVal = TimeUtils.minuteBefore(toVal, Constants.getApiServerConf("master.metrics.space-time").toInt)
    }

    metricPhase match {
      case Some(mPhase) =>
        metricType match {
          case Some(mType) =>
            get(configId, mPhase, mType, fromVal, toVal)
          case None =>
            get(configId, mPhase, fromVal, toVal)
        }
      case None =>
        get(configId, fromVal, toVal)
    }
  }

  def get(configId: String, from: String, to: String): List[MetricInfo] = {
    query(SELECT_SQL)(Array(configId, from, to)).toList
  }

  def get(configId: String, metricPhase: MetricPhase, from: String, to: String): List[MetricInfo] = {
    MetricType.values.flatMap(get(configId, metricPhase, _, from, to)).toList
  }

  def get(configId: String, metricPhase: MetricPhase, metricType: MetricType, from: String, to: String): List[MetricInfo] = {
    query(TYPE_SELECT_SQL)(Array("-", configId, metricType.toString, metricPhase.toString, from, to)).toList
  }

  /*def get(id: String, mType: Option[String], from: Option[String], to: Option[String]): List[MetricInfo] = {

    logger.debug(s"query metric for channel[$id]")
    var fromVal: String = ""
    var toVal: String = ""
    var ms: com.haima.sage.bigdata.etl.common.model.Stream[MetricInfo] = null
    var metrics: List[MetricInfo] = List[MetricInfo]()
    // toVal 默认为currentDate
    to match {
      case Some(v) => toVal = v
      case None => toVal = TimeUtils.defaultFormat(TimeUtils.currentDate)
    }
    // fromVal 默认为toVal之前的10分钟
    from match {
      case Some(v) => fromVal = v
      case None => fromVal = TimeUtils.minuteBefore(toVal, Constants.getApiServerConf("master.metrics.space-time").toInt)
    }
    mType match {
      case Some(v) =>
        ms = query(TYPE_SELECT_SQL)(Array("-", id, v.toUpperCase, fromVal, toVal))
      case None =>
        ms = query(SELECT_SQL)(Array("-", id, fromVal, toVal))
    }
    while (ms.hasNext) {
      metrics = ms.next() :: metrics
    }
    metrics
  }*/

  def getByMetricId(id: String): Option[MetricInfo] = {
    one(SELECT_BY_METRIC_ID)(Array(id))
  }

  def getByMetricId(metricId: String, from: String, to: String): List[MetricInfo] = {
    query(SELECT_METRICS_BY_METRIC_ID)(Array(metricId, from, to)).toList
  }

  override def entry(set: ResultSet): MetricInfo = {
    MetricInfo(set.getString("ID"), set.getString("METRIC_ID"), set.getString("CONFIG_ID"),
      MetricType.withName(set.getString("METRIC_TYPE")),
      MetricPhase.withName(set.getString("METRIC_PHASE")),
      mapper.readValue[Map[String, Any]](set.getString("METRIC")),
      Option(set.getTimestamp("LAST_TIME")), Option(set.getTimestamp("CREATE_TIME")))
  }

  /*
  * id 是必选字段 可以为空
  *   并且id应该为最后一个字段
  *
  * */
  override def from(entity: MetricInfo): Array[Any] = Array(entity.collectorId, entity.metricId, entity.configId, entity.lasttime, entity.metric, entity.metricType, entity.metricPhase)

  def where(entity: MetricInfo): String = {
    ""
  }
}
