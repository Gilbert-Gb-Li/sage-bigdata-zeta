package com.haima.sage.bigdata.etl.stream


import java.sql._
import java.text.{DateFormat, SimpleDateFormat}
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{JDBCSource, RichMap, Stream}
import com.haima.sage.bigdata.etl.driver.JDBCDriver
import javax.activation.UnsupportedDataTypeException

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object JDBCStream {


  def apply(conf: JDBCSource): JDBCStream = {
    new JDBCStream(conf)
  }


  class JDBCStream(conf: JDBCSource) extends Stream[RichMap](None) {

    val driver = JDBCDriver(conf)

    val value: Long = conf.start.toString.toLong
    val step: Long = conf.step
    val timeout: Long = conf.timeout


    private var _info: String = ""
    private var conn: Connection = _
    private var item: RichMap = _

    private final val sdf = new ThreadLocal[DateFormat]() {
      protected override def initialValue(): DateFormat = {
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      }
    }


    private var statement: PreparedStatement = _


    implicit def toLong(data: Any): Long = data match {
      case last: java.util.Date if value < last.getTime =>
        last.getTime - 1
      case last: Int if value < last =>
        last.toLong - 1
      case last: Long if value < last =>
        last - 1
      case last: BigDecimal if value < last =>
        last.toLong - 1
      case last: java.math.BigDecimal if value < last.longValue() =>
        last.longValue() - 1
      case last: String if last.matches("\\d+") && value < last.toLong =>
        try {
          last.toLong - 1
        } catch {
          case e: Exception =>
            logger.debug(s"some error $e")
            value
        }
      case last: String if last.matches("\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}") =>
        try {
          val data = sdf.get().parse(last).getTime
          if (value < data) {
            data - 1
          } else {
            value
          }

        } catch {
          case e: Exception =>
            logger.debug(s"some error $e")
            value
        }
      case _ =>
        value
    }

    var current: Long = min().toLong

    logger.info(s"from[$value]")
    private var set: ResultSet = _


    override def toString(): String = {
      conf.uri
    }

    var count = 0

    @tailrec
    private def autoConnect(): Connection = {
      driver.driver() match {
        case Success(connection) =>
          count = 0
          connection
        case Failure(e) =>
          count += 1
          _info = s"jdbc get connect error:$e "
          logger.error(_info)
          TimeUnit.MILLISECONDS.sleep(timeout)
          if (isRunning && count < 2) {
            autoConnect()
          } else {
            throw e
          }
      }
    }


    private def connection: Connection = synchronized {
      try {
        if (conn == null) {
          conn = autoConnect()
          conn
        } else if (!conn.isValid(10)) {
          conn.close()
          conn = autoConnect()
          conn
        } else {
          conn
        }
      } catch {
        case _: AbstractMethodError =>
          conn.close()
          conn = autoConnect()
          conn
        case _: Throwable =>
          conn.close()
          conn = autoConnect()
          conn
      }
    }


    def max(): Any = {
      var statement_max: PreparedStatement = null
      var set: ResultSet = null
      val value = try {
        statement_max = connection.prepareStatement(conf.MAX, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        set = statement_max.executeQuery()
        if (set.next()) {
          set.getObject(1)
        } else {
          current
        }
      } catch {
        case e: Exception =>
          logger.error(s"get max error :$e")
          current
      } finally {
        try {
          if (set != null)
            set.close()
          set = null
        } catch {
          case e: Exception =>
            logger.debug(s"error when close $e")
        }
        try {
          if (statement_max != null) {
            statement_max.close()
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            logger.error(s"max close statement  error :$e")
        }
      }
      value
    }

    def min(): Any = {
      var statement_max: PreparedStatement = null
      var set: ResultSet = null
      val value = try {
        statement_max = connection.prepareStatement(conf.MIN)
        set = statement_max.executeQuery()
        if (set.next()) {
          val min = set.getObject(1)
          logger.debug(s"get min returns new record:$min")
          min
        } else {
          current
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error(s"get min error :$e")
          current
      } finally {
        try {
          if (set != null)
            set.close()
          set = null
        } catch {
          case e: Exception =>
            logger.debug(s"error when close $e")
        }
        try {
          if (statement_max != null) {
            statement_max.close()
          }
        } catch {
          case e: Exception =>
            logger.error(s"min close statement error :$e")
        }
      }
      value
    }

    /**
      * 根据时间的检查列，判断还有没有新的数据， 有的话初始化数据查询语句，否则不处理（多次检查后抛出异常）
      **/
    implicit def time(last: Long): Boolean = {
      logger.debug(s"current is ${sdf.get.format(current)}, max is ${sdf.get.format(last)}")
      if (last <= current) {
        current = last
        false
      } else {
        statement = connection.prepareStatement(conf.SELECT, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        statement.setTimestamp(1, new Timestamp(current))
        current = current + step * 1000
        statement.setTimestamp(2, new Timestamp(current))

        true
      }
    }

    /**
      * 根据检查列，判断还有没有新的数据，有的话初始化数据查询语句，否则不处理（多次检查后抛出异常）
      **/
    implicit def tupled(last: Long): Boolean = {
      logger.debug(s"current is $current, max is $last")
      if (last <= current) {
        current = last
        false
      } else {
        statement = connection.prepareStatement(conf.SELECT, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        statement.setLong(1, current)
        current = current + step
        statement.setLong(2, current)
        true
      }
    }

    def hasMore: Boolean = {
      max() match {
        case last: java.sql.Timestamp =>
          time(last.getTime)
        case last: java.util.Date =>
          time(last.getTime)
        case last: Int =>
          tupled(last)
        case last: Long =>
          tupled(last)
        case last: BigDecimal =>
          tupled(last.toLong)
        case last: java.math.BigDecimal =>
          tupled(last.longValue())
        case last: String if last.matches("\\d+") =>
          try {
            tupled(last.toLong)
          } catch {
            case e: Exception =>
              logger.debug(s"some error $e")
              false
          }
        case last: String if last.matches("\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}") =>
          try {
            time(sdf.get().parse(last).getTime)
          } catch {
            case e: Exception =>
              logger.debug(s"some error $e")
              false
          }
        case last: String =>
          try {
            tupled(last.toLong)
          } catch {
            case e: Exception =>
              logger.debug(s"some error $e")
              false
          }
        case msg =>
          logger.error(s"Unsupported DataType:$msg -> ${msg.getClass}")
          throw new UnsupportedDataTypeException(s"sql type:$msg")
      }
    }

    private def query(): ResultSet = {
      if (!hasMore) {
        logger.debug(s"has no more data, thread will sleep for $timeout milliseconds")
        TimeUnit.MILLISECONDS.sleep(timeout)
        if (!hasMore) {
          throw new TimeoutException(s"after $timeout ms jdbc read timeout!")
        }
      }
      statement.setFetchDirection(ResultSet.FETCH_FORWARD)
      if (!conf.driver.contains("com.sybase.jdbc3.jdbc.SybDriver")) {
        statement.setFetchSize(10000)
      }
      val resultSet: ResultSet = statement.executeQuery()
      resultSet //返回结果
    }

    @tailrec
    final def hasNext: Boolean = {
      state match {
        case State.ready =>
          true
        case State.done =>
          if (set == null) {
            closeStmt()
            if (connection != null && !connection.isClosed) {
              connection.close()
            }
            false
          } else {
            makeData()
            hasNext
          }
        case State.fail =>
          false
        case _ =>
          if (set == null) {
            set = query()
          }
          makeData()
          hasNext
      }
    }


    def makeData() {
      if (set == null) {
        //  init()
      } else if (set.next()) {
        try {
          val meta = set.getMetaData
          item = (1 to meta.getColumnCount)
            .map { i =>
              val data = Try(set.getObject(i))
              if (data.isSuccess) {
                (meta.getTableName(i), meta.getColumnName(i), data.get)
              } else {
                logger.debug("makeData get null, now index is " + i)
                (meta.getTableName(i), meta.getColumnName(i), null)
              }
            }.groupBy(_._1) match {
            case data =>
              data.flatMap(_._2.map(row => {
                row._3 match {
                  case d: java.sql.Timestamp =>
                    (row._2, new java.util.Date(d.getTime))
                  case d: java.math.BigDecimal =>
                    (row._2, d.doubleValue())
                  case d: java.math.BigInteger =>
                    (row._2, d.longValue())
                  case d: scala.math.BigDecimal =>
                    (row._2, d.doubleValue())
                  case d: scala.math.BigInt =>
                    (row._2, d.longValue())
                  case d =>
                    (row._2, d)
                }
              }
              )).toMap

            //              if (data.size == 1) {
            //            TODO multi tables
            //              } else {
            //                val pah = data.map {
            //                  case (tableName, d) =>
            //                    (tableName, d.map(row => {
            //                      row._3 match {
            //                        case d: java.sql.Timestamp =>
            //                          (row._2, new java.util.Date(d.getTime))
            //                        case _d =>
            //                          (row._2, _d)
            //                      }
            //                    }).toMap)
            //                }
            //
            //
            //                val pah1 = pah - ""
            //                if (pah1.size == 1) {
            //                  pah1.head._2.toMap.++(pah.getOrElse("", Map()).asInstanceOf[java.util.Map[String, Any]])
            //                } else {
            //                  /*
            //                  *
            //                  * 当是视图时,或者多表联合查询时,""表示不属于任何表的数据;
            //                  * */
            //                  pah.get("") match {
            //                    case Some(d)=>
            //                      pah1.++(d)
            //                    case _=>
            //                      pah1
            //                  }
            //                }
            //              }
          }
          if (state != State.done) {
            ready()
          }
          _info = ""
        } catch {
          case e: Exception =>
            _info = "sql close error"
            logger.error(s"SQL get row date error:$e")
            state = State.fail
        }
      } else {
        closeStmt()
      }
    }

    def closeStmt(): Unit = {
      try {
        if (set != null) {
          set.close()
          set = null
        }
        _info = ""
      }
      catch {
        case e: SQLException =>
          _info = "jdbc set close error"
          logger.error("JDBC set close error:{}", e)
      }
      try {
        if (statement != null) {
          statement.close()
        }
        _info = ""
      }
      catch {
        case e: SQLException =>
          _info = "jdbc statement close error"
          logger.error("JDBC statement close error:{}", e)
      }
    }

    final override def next(): RichMap = {
      init()
      item
    }

    def msg: String = _info
  }

}


