package com.haima.sage.bigdata.etl.writer

import java.io.IOException
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.{Calendar, Date}

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.exception.LogWriteException
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.writer.{JDBCField, NameFormatter}
import com.haima.sage.bigdata.etl.driver.JDBCDriver
import com.haima.sage.bigdata.etl.metrics.MeterReport

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


@SuppressWarnings(Array("serial"))
class JDBCLogWriter(conf: JDBCWriter, timer: MeterReport) extends DefaultWriter[JDBCWriter](conf, timer: MeterReport) with BatchProcess {

  val driver = JDBCDriver(conf)
  private final val formatter = NameFormatter(conf.table, conf.persisRef)
  private var existTables: Set[String] = Set()

  private var caches: List[Map[String, Any]] = Nil
  private val PrimaryKeys = ListBuffer[String]()

  /**
    * 建表语句中是否允许为空的处理
    *
    * @param field Field，数据表列
    * @return 拼接字符串
    */
  def nullable(field: JDBCField): String = field.nullable match {
    case Some(f) if "N".equals(f) =>
      "NOT NULL"
    case _ =>
      ""
  }

  /**
    * 构建列类型的字符创
    *
    * @param field JDBCField 列信息
    * @return String
    */
  def fieldType(field: JDBCField): String = {
    val length: Integer = if (field.length.nonEmpty) field.length.get else 0
    val decimal: Integer = if (field.decimal.nonEmpty) field.decimal.get else 0
    field.`type` match {
      case "BOOLEAN" => "BOOLEAN"
      case "CLOB" => "CLOB"
      case "TIMESTAMP" => "TIMESTAMP"
      case "INTEGER" => if (length > 0) s"INTEGER($length)" else "INTEGER(16)"
      case "DOUBLE" =>
        if (length > 0 && decimal > 0) s"DOUBLE($length,$decimal)"
        else if (length > 0 && decimal == 0) s"DOUBLE($length,2)"
        else "DOUBLE(20,2)"

      case "LONG" => if (length > 0) s"LONG($length)" else "LONG(20)"
      case "TEXT" => if (length > 0) s"TEXT($length)" else "TEXT(2000)"
      case "FLOAT" =>
        if (length > 0 && decimal > 0) s"FLOAT($length,$decimal)"
        else if (length > 0 && decimal == 0) s"FLOAT($length,2)"
        else "FLOAT(16,2)"
      case "BLOB" => "BLOB"
      case "CHAR" => if (length > 0) s"CHAR($length)" else "CHAR(25)"
      case "VARCHAR" => if (length > 0) s"VARCHAR($length)" else "VARCHAR(250)"
      case "VARCHAR2" => if (length > 0) s"VARCHAR2($length)" else "VARCHAR2(250)"
      case "NUMERIC" =>
        if (length > 0 && decimal > 0) s"NUMERIC($length,$decimal)"
        else if (length > 0 && decimal == 0) s"NUMERIC($length,0)"
        else "NUMERIC(16,0)"
      case "NUMBER" =>
        if (length > 0 && decimal > 0) s"NUMBER($length,$decimal)"
        else if (length > 0 && decimal == 0) s"NUMBER($length,0)"
        else "NUMBER(16,0)"
      case "DECIMAL" =>
        if (length != 0 && decimal > 0) s"DECIMAL($length,$decimal)"
        else if (length > 0 && decimal == 0) s"DECIMAL($length,2)"
        else "DECIMAL(20,2)"
    }
  }

  /**
    * 数据表列信息
    */
  var columns: Map[String, String] = conf.metadata match {
    case Some(_) =>
      try {
        conf.metadata.get.map(field => {
          if (field.pk.contains("Y"))
            PrimaryKeys.append(field.name.trim)
          (field.name.trim, s"${field.name.trim} ${fieldType(field)} ${nullable(field)}")
        }).toMap
      } catch {
        case e: Exception =>
          context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
          context.stop(self)
          throw e
      }
    case None =>
      context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, "jdbc table properties must be set !")
      context.stop(self)
      throw new Exception("jdbc table properties must be set !")
  }


  //表存在的情况下，需要从数据库获取表schema信息  todo fixed
  private var fields: List[String] = _

  val conn: Connection = connector()

  conn.setAutoCommit(false)
  // init()


  private def connector(): Connection = {
    driver.driver() match {
      case Success(connection) =>
        connection
      case Failure(e) =>
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
        context.stop(self)
        throw e;
    }
  }

  /**
    * 判断表是否存在
    *
    * @param tableName 表名称
    * @return Boolean true:存在  false:不存在
    */
  private def validateTableExist(tableName: String): Boolean = {
    if (existTables.contains(tableName)) {
      true
    } else {
      try {
        conn.getMetaData.getTables(null, null, tableName, Array("TABLE")).next()
      } catch {
        case _: Exception =>
          false
      }
    }
  }

  /**
    * 创建数据表
    *
    * @param table 表名称
    * @return 创建是否成功
    */
  def create(table: String): Boolean = {
    if (PrimaryKeys.nonEmpty) {
      columns += ("PRIMARY KRY" -> s"CONSTRAINT pk PRIMARY KEY(${PrimaryKeys.toArray.mkString(",")})")
    }
    val create_sql = s"CREATE TABLE $table(${columns.values.mkString(",")})"
    logger.debug(s"[ " + create_sql.toLowerCase + s" ] ")
    val statement = conn.prepareStatement(create_sql)
    try {
      statement.execute()
      existTables += table
      true
    } catch {
      case e: Exception =>
        logger.error(s"jdbc create table error :$e")
        throw e
    } finally {
      statement.close()
    }

  }

  /**
    * 表存在的情况下，获取表的元信息
    *
    * @param tableName 表名
    * @return 列列表
    */
  def getMetadata(tableName: String): List[String] = {
    //忽略掉多余字段
    var sm: PreparedStatement = null
    var set: ResultSet = null
    try {
      sm = conn.prepareStatement(s"SELECT * FROM $tableName where 1=2")
      set = sm.executeQuery()
      val meta = set.getMetaData
      (1 to meta.getColumnCount)
        .map { i =>
          meta.getColumnName(i).toUpperCase
        }.toList
    } catch {
      case e: Exception =>
        throw e
    } finally {
      if (set != null && !set.isClosed)
        set.close()
      if (sm != null && !sm.isClosed)
        sm.close()
    }
  }

  @throws[IOException]
  def flush() {
    try {
      conn.commit()
      report()
      logger.debug(s"jdbc commit[${caches.size}] ")
      caches = Nil
    } catch {
      case e: Exception =>
        logger.error(s"jdbc save data has  error :$e")
        self ! Status.LOST_CONNECTED
    }
  }

  @throws(classOf[LogWriteException])
  def write(t: RichMap): Unit = {
    val _data = t
    val table = formatter.format(_data) //获取表名
    Try {
      if (!validateTableExist(table)) {
        conf.protocol match {
          case "phoenix" =>
          case _ => create(table)
        }
      }
      fields = getMetadata(table) //获取表的元信息
    } match {
      case Success(_) =>
        val data: Map[String, Any] = _data.filter { //过滤掉数据key在表中不存在的列
          case (key, value) =>
            (value match {
              case d: Date =>
                val calendar = Calendar.getInstance()
                calendar.setTime(d)
                calendar.get(Calendar.YEAR) >= 1970
              case _ =>
                value != null
            }) && (fields.contains(key.trim.toLowerCase)
              || fields.contains(key.trim.toUpperCase())
              || fields.contains(key.trim.toLowerCase.replaceAll("`", ""))
              || fields.contains(key.trim.toUpperCase.replaceAll("`", ""))
              || fields.contains(key.trim.toUpperCase.replaceAll("\"", ""))
              || fields.contains(key.trim.toLowerCase.replaceAll("\"", ""))
              || fields.contains(key.trim.toLowerCase.replaceAll("'", ""))
              || fields.contains(key.trim.toUpperCase.replaceAll("'", ""))
              ) //todo fixed 确定大小写敏感问题的处理方案
        }
        val statement = prepare(data, table)
        caches = t :: caches
        try {
          statement.execute()
        } catch {
          case e: Exception =>
            logger.error(s"for ($data): $e")
            context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
            context.stop(self)
          //todo fixed 确定插入的错误数据的处理方式
        } finally {
          statement.closeOnCompletion()
        }
        if (caches.size == conf.cache) {
          flush()
        }
      case Failure(e) => //建表失败
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
        context.stop(self)
    }
  }


  def prepare(data: Map[String, Any], table: String): PreparedStatement = {
    val schema = conf.schema //表的schema信息
    conf.protocol match {
      case "phoenix" =>
        val insert = s"upsert into $schema.$table${data.keys.mkString("(", ",", ")")} values${
          data.keys.toList.map(_ => "?").mkString("(", ",", ")")
        }"
        val statement = conn.prepareStatement(insert)
        data.values.zipWithIndex.foreach { case (da, index) => statement.setObject(index + 1, da) }
        statement
      case "mysql" =>
        if (PrimaryKeys.nonEmpty) {
          val fields: Seq[String] = (data.keys.toSet -- PrimaryKeys.toSet).toList
          val insert = s"insert into $schema.$table${data.keys.mkString("(", ",", ")")} values${
            data.keys.toList.map(_ => "?").mkString("(", ",", ")")
          } ON DUPLICATE KEY UPDATE ${fields.map(x => x + " = ?").mkString(",")}"
          //logger.debug(s"insert:$insert")
          val statement = conn.prepareStatement(insert)
          val map = data.filter { case (key, value) =>
            (value != null) && (fields.contains(key.trim.toLowerCase)) //todo fixed 确定大小写敏感问题的处理方案
          }
          //logger.debug(s"insert data :${data.values ++ map.values}")
          (data.values ++ map.values).zipWithIndex.foreach { case (da, index) => statement.setObject(index + 1, da) }
          statement
        } else {
          val insert = s"insert into $schema.$table${data.keys.mkString("(", ",", ")")} values${
            data.keys.toList.map(_ => "?").mkString("(", ",", ")")
          }"
          val statement = conn.prepareStatement(insert)
          data.values.zipWithIndex.foreach { case (da, index) => statement.setObject(index + 1, da) }
          statement
        }
      case _ =>
        val insert = s"insert into $schema.$table${data.keys.mkString("(", ",", ")")} values${
          data.keys.toList.map(_ => "?").mkString("(", ",", ")")
        }"
        val statement = conn.prepareStatement(insert)
        data.values.zipWithIndex.foreach { case (da, index) => statement.setObject(index + 1, da) }
        statement
    }
  }

  /**
    * 关闭writer
    *
    * @throws java.io.IOException IO异常
    */
  @throws(classOf[IOException])
  def close() {
    if (connect || caches == null || caches.isEmpty) {
      this.flush()
      context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
      context.stop(self)
    } else {
      caches.foreach(self !)
      caches = null
      self ! (self.path.toString, STOP)
    }
  }

}