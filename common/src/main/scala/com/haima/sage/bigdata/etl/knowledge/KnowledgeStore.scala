package com.haima.sage.bigdata.etl.knowledge

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}

import com.haima.sage.bigdata.etl.common.model.{Field, Schema}
import com.haima.sage.bigdata.etl.store.Store
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

trait KnowledgeStore {

  def operate:String //表的操作类型

  def tableSchema:Option[Schema] //表的schame

  private lazy val logger: Logger = LoggerFactory.getLogger(classOf[KnowledgeStore])
  private lazy val stores = Store()

  lazy val table: String = "\""+tableSchema.get.name+"\""  //获取表名，需要TODO FIX
  private lazy val conn: Connection = stores.connection
  /**
    * 判断表是否存在
    * @return Boolean
    */
  def exist(): Boolean = {
    try {
      conn.getMetaData.getTables(null, null, table.replaceAll("\"",""), Array("TABLE")).next()
    } catch {
      case _: Exception =>
        false
    }
  }

  /**
    * 删除数据表
     */
  def drop(): Unit = {
    Try{
      conn.createStatement().execute(s"drop table $table")
      commit()
    } match {
      case Success(_)=>
        logger.debug(s"drop table $table success !")
      case Failure(e)=>
        throw e
    }

  }

  /**
    * 建表语句中默认值的处理
    * @param field Field 数据表列
    * @return  拼接字符串
    */
  def default(field:Field) :String =
    field.default match {
      case Some(_)=>
        field.`type`.id match {
        case 6=>
        s"default('${field.default.get}')"
        case _=>
        s"default(${field.default.get})"
      }
      case None=>    ""
  }

  /**
    * 建表语句中是否允许为空的处理
    * @param field  Field，数据表列
    * @return 拼接字符串
    */
  def nullable(field:Field):String= field.nullable match {
      case Some(f) if "N".equals(f)=>
        "NOT NULL"
      case _=>
        ""
    }

  /**
    * 创建表
    * @return Boolean 成功与否
    */
  def create(): Boolean = {
    val  PrimaryKeys = ListBuffer[String]()
    //对于解析规则和离线建模两种情况进行处理
    var field_name=""
    var columns: Map[String, String] = tableSchema match {
      case Some(_)=>
        tableSchema.get.fields.map(field=>{
          if(field.pk.contains("Y"))
            PrimaryKeys.append(field.name)
           field_name="\""+field.name+"\""
          (field.name,s"${field_name} ${field.`type`} ${nullable(field)} ${default(field)}")
        }).toMap

      case None=>
        Map()
    }

    if(PrimaryKeys.nonEmpty){
      columns += ("PRIMARY KRY"->s"CONSTRAINT pk PRIMARY KEY(${PrimaryKeys.toArray.mkString(",")})")
    }

    val create_sql = s"CREATE TABLE $table(${columns.values.mkString(",")})"
    logger.debug(s"[ " + create_sql.toLowerCase + s" ] " )
      val statement = conn.prepareStatement(create_sql)

    try {
      statement.execute()
      true
    } catch {
      case e: Exception =>
       e.printStackTrace()
        false
    } finally {
      statement.closeOnCompletion()
      commit()
    }

  }

  /**
    * 表的更新或是创建操作
    * @param `type` "CREATE":新增 "UPDATE"：更新 默认值是"CREATE"
    * @return
    */
  def operate(`type`:String):Boolean ={
    Try{
      `type` match {
        case "UPDATE"=>
          if (!exist()) //表存在
            throw new Exception(s"The table[$table]  does not exist")
        case "CREATE" =>
          if(exist())//判断是否存在数据表
            drop()
          if(!create())
            throw new Exception(s"create table[$table]  fail")
        case _=>
          throw new Exception(s"unknown operation [${ `type`}]  does not exist")
        }
    } match {
      case Success(_)=>
        true
      case Failure(e)=>
        e.printStackTrace()
        false
    }
  }

  /**
    * 插入数据
    * @param _data Map[String, Any]
    */
  def insert(_data: Map[String, Any]): Unit = {
    val insert = s"insert into $table${_data.keys.toList.map(x=>"\""+x+"\"").mkString("(", ",", ")")} values${
      _data.keys.toList.map(_ => "?").mkString("(", ",", ")")
    }"
    //logger.debug(s"[ " + insert.toLowerCase + s" ] " )
    val statement = conn.prepareStatement(insert)
    _data.values.zipWithIndex.foreach { case (da, index) => statement.setObject(index + 1, da) }
    try {
      statement.execute()
    } catch {
      case e: Exception =>
        throw e
    } finally {
      statement.closeOnCompletion()
      commit()
    }
  }

  def commit(): Unit = {
    conn.commit()
  }

  def close():Unit={  }

  def filterData(_data: Map[String, Any]):Map[String, Any]={
    //忽略掉多余字段
    var sm: PreparedStatement=null
    var set: ResultSet=null
    try{
      sm = conn.prepareStatement(s"SELECT * FROM $table where 1=2")
      set = sm.executeQuery()

      val meta = set.getMetaData
      val columns = (1 to meta.getColumnCount)
        .map { i =>
          meta.getColumnName(i).toUpperCase
        }.toArray
      _data.filter(data=>{
        columns.contains(data._1.replaceAll("\"","").replaceAll("\'","").toUpperCase)
      })
    }catch {
      case e:Exception=>
        throw e
    }finally {
      if(set !=null && !set.isClosed)
        set.close()
      if(sm !=null && !sm.isClosed)
        sm.close()
      commit()
    }
  }
}