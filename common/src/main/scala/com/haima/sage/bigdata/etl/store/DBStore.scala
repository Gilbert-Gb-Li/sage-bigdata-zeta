package com.haima.sage.bigdata.etl.store

import java.sql.{Clob, Connection, Date, DriverManager, PreparedStatement, ResultSet, SQLException, Timestamp}
import java.util.concurrent.locks.ReentrantLock

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{Pagination, Stream}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
  * Created by zhhuiyan on 15/3/9.
  */


object Store {


  private val lock = new ReentrantLock()


  private val driver: String = Try(Constants.CONF.getString("app.store.driver.driver")).toOption.orNull

  private val url: String = Try(Constants.CONF.getString("app.store.driver.url")).toOption.orNull

  private val user: String = Try(Constants.CONF.getString("app.store.driver.user")).toOption.orNull

  private val password: String = Try(Constants.CONF.getString("app.store.driver.password")).toOption.orNull

  private val derby_log_location: String = Try(Constants.CONF.getString("app.store.log.location")).toOption.orNull


  private[Store] var conn: Connection = _


  def connect(): Option[Connection] = {
    Try {
      Some(try {
        println(s"derby connect to ${url}")
        DriverManager.getConnection(url, user, password)
      } catch {
        case e: Throwable =>
          DriverManager.getConnection(url, user, password)
      })
    }.getOrElse(None)
  }

  def apply(): Store = {
    lock.lock()
    if (conn == null) {
      System.setProperty("derby.stream.error.file", derby_log_location)
      System.setProperty("derby.storage.pageCacheSize", "10000")
      System.setProperty("derby.storage.pageSize", "8192")
      Class.forName(driver)
      conn = connect().get
    }
    lock.unlock()
    new Store()
  }

  class Store {

    def connection: Connection = {
      if (conn.isClosed)
        conn = connect().get
      conn.setAutoCommit(false)
      conn
    }

    def close(): Unit = {
      conn.close()
    }
  }

}

object ClobAsString {
  implicit def apply(clob: Clob): ClobAsString = new ClobAsString(clob)
}

class ClobAsString(clob: Clob) {
  def asString(): String = {
    import java.io._
    val is = clob.getCharacterStream(); // 得到流
    val lines = new BufferedReader(is)
    try {
      if (lines == null) {
        ""
      } else {

        val line = new StringBuffer("")
        var t: String = null
        while ( {
          if (lines != null)
            t = lines.readLine()
          else {
            t = null
          }
          t != null
        }) {
          line.append(t)
        }
        line.toString
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        ""
    } finally {
      if (lines != null)
        lines.close()
      if (is != null)
        is.close()
    }


  }

}

trait DBStore[T >: Null, ID] extends AutoCloseable with BaseStore[T, ID] with Mapper {
  val logger: Logger = LoggerFactory.getLogger("com.haima.sage.bigdata.etl.store.DBStore")
  val store = Store()


  protected def TABLE_NAME: String

  protected def CREATE_TABLE: String

  protected def CHECK_TABLE: String = s"UPDATE $TABLE_NAME SET ID='1' WHERE 1=3"

  protected def SELECT_BY_ID = s"SELECT * FROM $TABLE_NAME WHERE ID=?"

  protected def SELECT_ALL = s"SELECT * FROM $TABLE_NAME"

  protected def SELECT_COUNT = s"SELECT COUNT(ID) FROM $TABLE_NAME"


  protected def INSERT: String

  protected def UPDATE: String

  protected def DELETE = s"DELETE FROM $TABLE_NAME WHERE ID=?"


  override def metadata(): Map[String, String] = {
    val statement: PreparedStatement = from(SELECT_ALL, Array())
    val set: ResultSet = statement.executeQuery()
    val meta = set.getMetaData
    val rt = (1 to meta.getColumnCount)
      .map { i =>
        (meta.getColumnName(i), meta.getColumnClassName(i))
      }.toMap
    set.close()
    statement.close()
    connection.commit()
    rt
  }

  def init: Boolean = {
    val lock = new ReentrantLock()
    lock.lock()
    val flag = if (exist) {
      true
    } else {
      execute(CREATE_TABLE)()
    }
    lock.unlock()
    flag
  }


  def connection: Connection = {
    store.connection
  }

  protected def entry(set: ResultSet): T

  /*
  * id 是必选字段 可以为空
  *   并且id应该为最后一个字段
  *
  * */
  protected def from(entity: T): Array[Any]

  protected def where(entity: T): String

  override def set(entity: T): Boolean = {

    val array = from(entity)
    if (exist(SELECT_BY_ID)(Array(array(array.length - 1))))
      execute(UPDATE)(array)
    else
      execute(INSERT)(array)
  }

  override def delete(id: ID): Boolean =
    if (execute(DELETE)(Array(id))) true else false

  override def get(id: ID): Option[T] =
    one(SELECT_BY_ID)(Array(id))


  override def all(): List[T] = query(SELECT_ALL)().toList

  override def queryByPage(start: Int, limit: Int, orderBy: Option[String], order: Option[String], sample: Option[T]): Pagination[T] = {
    val w = sample match {
      case Some(entry) if entry.isInstanceOf[T] =>
        where(entry.asInstanceOf[T])
      case _ =>
        ""
    }

    val total = count(SELECT_COUNT + w)(Array())
    val fetch = s" ORDER BY ${orderBy.getOrElse("createtime")} ${order.getOrElse("desc")} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"

    Pagination[T](start, limit, total, result = query(SELECT_ALL + w + fetch)(Array(start, limit)).toList)
  }


  protected def from(sql: String, array: Array[Any] = Array()): PreparedStatement = {
    val stmt = connection.prepareStatement(sql)
    if (array.length > 0) {
      (1 to array.length).foreach {
        i =>
          array(i - 1) match {
            case a: Int => stmt.setInt(i, a)
            case a: Integer => stmt.setInt(i, a);
            case a: Long => stmt.setLong(i, a)
            case a: String => stmt.setString(i, a)
            case a: Boolean => stmt.setBoolean(i, a)
            case a: Double => stmt.setDouble(i, a)
            case a: Byte => stmt.setByte(i, a)
            case a: Short => stmt.setShort(i, a)
            case a: Timestamp => stmt.setTimestamp(i, a)
            case a: Date => stmt.setTimestamp(i, new Timestamp(a.getTime))
            case a: java.util.Date => stmt.setTimestamp(i, new Timestamp(a.getTime))
            case null => stmt.setObject(i, null)
            case ac => stmt.setObject(i, ac)
              logger.error("unmatch type! type : " + ac.getClass)
          }
      }
    }
    stmt
  }

  def count(sql: String)(array: Array[Any] = Array()): Int = {
    var statement: PreparedStatement = null
    var set: ResultSet = null
    try {
      statement = from(sql, array)
      set = statement.executeQuery()
      if (set.next()) {
        set.getInt(1)
      } else {
        0
      }
    } catch {
      case e: SQLException =>
        logger.error(s"count error:${e.getSQLState},${e.getSQLState}")
        throw e
      case e: Throwable => e.printStackTrace()
        throw e
    } finally {
      if (set != null && !set.isClosed)
        set.close()
      if (statement != null && !statement.isClosed)
        statement.close()
      connection.commit()
    }
  }

  def query(sql: String)(array: Array[Any] = Array()): Stream[T] = {
    try
      new Stream[T](None) {


        override protected def loggerName: String = "DBStore"

        private var item: T = _
        val statement: PreparedStatement = from(sql, array)
        val set: ResultSet = statement.executeQuery()

        override def hasNext: Boolean = {

          state match {
            case State.ready =>
              true
            case State.done =>
              this.close()
              false
            case State.fail =>
              this.close()
              false
            case _ =>
              makeData()
              hasNext
          }
        }

        def makeData() {
          try {
            if (set.next()) {
              item = entry(set)
              ready()
            } else {
              finished()
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
              logger.error(s"convert object error$e")
              state = State.fail
          }

        }

        override def close(): Unit = {
          super.close()
          set.close()
          statement.close()

        }

        override def next(): T = {
          init()
          item
        }
      }

    catch {
      case e: SQLException =>
        logger.error(s"query error:${e.getErrorCode},${e.getSQLState}")
        throw e
      case e: Throwable => e.printStackTrace()
        throw e
    }
  }

  def execute(sql: String)(array: Array[Any] = Array[Any]()): Boolean = {
    var statement: PreparedStatement = null
    Try {
      try {
          statement = from(sql, array)
        statement.execute()

        true
      } catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error(s"db store execute[$sql] has error:$e")
          throw e
      } finally {
        if (statement != null)
          statement.close()
        connection.commit()
      }
    }.getOrElse(false)

  }

  def update(sql: String)(array: Array[Any] = Array()): Int = {
    var statement: PreparedStatement = null
    Try {
      try {
        statement = from(sql, array)
        statement.executeUpdate()
      } finally {
        if (statement != null)
          statement.close()
        connection.commit()
      }

    }.getOrElse(-1)

  }

  def exist: Boolean = {
    var statement: PreparedStatement = null
    try {
      logger.debug(s"check is $CHECK_TABLE")
      statement = connection.prepareStatement(CHECK_TABLE)
      statement.execute()
      true
    } catch {
      case exception: SQLException =>
        exception.getSQLState match {
          case "42X05" =>
            false
          case "42X14" =>
            logger.error(s"42X14 $CHECK_TABLE  :${exception.getErrorCode},${exception.getNextException}")
            true
          case "42821" =>
            logger.error(s"42821 $CHECK_TABLE :${exception.getErrorCode},${exception.getNextException}")
            false
          case msg =>
            logger.error(s"error:$CHECK_TABLE Unhandled SQLException :$msg,$CHECK_TABLE")
            false
        }

    } finally {
      if (statement != null)
        statement.close()
    }

  }

  def exist(sql: String)(array: Array[Any] = Array()): Boolean = {
    var result: ResultSet = null
    var statement: PreparedStatement = null

    try {
      statement = from(sql, array)

      result = statement.executeQuery


      if (result.next) {
        true
      }
      else false
    } catch {
      case e: Exception =>
        logger.error("get read_position store db error:{}", e)
        false
    } finally {
      if (result != null) try {
        result.close()
      }
      catch {
        case e: SQLException =>
          logger.error("close result error:{}", e)
      }
      if (statement != null) {
        try {
          statement.close()
        }
        catch {
          case e: SQLException =>
            logger.error("close stmt error:{}", e)
        }
      }
    }
  }

  /* def exit(sql: String)(array: Array[Any] = Array()): Boolean = {
     var result: ResultSet = null
     var statement: PreparedStatement = null

     try {
       statement = from(sql, array)
       result = statement.executeQuery
       if (result.next) {
         true
       }
       else false
     } catch {
       case e: Exception =>
         logger.error("exit  store db error:{}", e)
         false
     } finally {
       if (result != null) try {
         result.close()
       }
       catch {
         case e: SQLException =>
           logger.error("close result error:{}", e)
       }
       if (statement != null) {
         try {
           statement.close()
         }
         catch {
           case e: SQLException =>
             logger.error("close stmt error:{}", e)
         }
       }
     }
   }*/

  def one(sql: String)(array: Array[Any] = Array()): Option[T] = {
    var result: ResultSet = null
    var statement: PreparedStatement = null

    try {
      statement = from(sql, array)
      result = statement.executeQuery
      if (result.next) {
        Some(entry(result))
      }
      else None
    }
    catch {
      case e: SQLException =>
        logger.error(s"get one from  store[$TABLE_NAME]  error:$e")
        None
    } finally {
      if (result != null) try {
        result.close()
      }
      catch {
        case e: SQLException =>
          logger.error("close result error:{}", e)
      }
      if (statement != null) {
        try {
          statement.close()
        }
        catch {
          case e: SQLException =>
            logger.error("close stmt error:{}", e)
        }
      }
    }
  }

  override def close(): Unit = {
    store.close()
  }

  def querys(sql: String)(array: Array[Any] = Array()): Option[List[T]] = {
    var result: ResultSet = null
    var statement: PreparedStatement = null
    var results = List[T]()
    try {
       statement = from(sql, array)
      result = statement.executeQuery
      while (result.next) {
        results = entry(result) :: results
      }
    }
    catch {
      case e: SQLException =>
        logger.error("get read_position store db error:{}", e)
    } finally {
      if (result != null) try {
        result.close()
      }
      catch {
        case e: SQLException =>
          logger.error("close result error:{}", e)
      }
      if (statement != null) {
        try {
          statement.close()
        }
        catch {
          case e: SQLException =>
            logger.error("close stmt error:{}", e)
        }
      }
    }
    Some(results)
  }
}

