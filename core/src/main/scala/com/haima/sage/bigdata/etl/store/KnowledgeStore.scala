package com.haima.sage.bigdata.etl.store

import java.sql.{PreparedStatement, ResultSet}
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model.Status
import com.haima.sage.bigdata.etl.knowledge.Knowledge
import com.haima.sage.bigdata.etl.utils.Mapper
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Dell on 2017-07-27.
  */
class KnowledgeStore extends DBStore[Knowledge,String] with Mapper{
  override val logger: Logger = LoggerFactory.getLogger("KnowledgeStore")

  protected var configs:Map[String,Knowledge]=Map()
  override val TABLE_NAME = "KNOWLEDGE"
  protected val CREATE_TABLE:String = s""" CREATE TABLE $TABLE_NAME(
                                                 |ID VARCHAR(64) PRIMARY KEY NOT NULL,
                                                 |NAME VARCHAR(64),
                                                 |DRIVERID VARCHAR(64),
                                                 |COLLECTORID VARCHAR(64),
                                                 |PARSERID VARCHAR(64),
                                                 |STATUS VARCHAR(64),
                                                 |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                 |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                 |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin
  override val SELECT_BY_ID: String = s"SELECT * FROM $TABLE_NAME WHERE ID=?"
  override val INSERT: String = s"INSERT INTO $TABLE_NAME(NAME, DRIVERID, COLLECTORID, PARSERID, STATUS, ID) VALUES(?,?,?,?,?,?) "
  override val UPDATE: String = s"UPDATE $TABLE_NAME set NAME=?, DRIVERID=?, COLLECTORID=?, PARSERID=?, STATUS=?, LAST_TIME=? WHERE ID=?"
  override val DELETE: String = s"DELETE FROM $TABLE_NAME WHERE ID=?"
  override val CHECK_TABLE: String = s"UPDATE $TABLE_NAME SET ID='1' WHERE 1=3"

  val SELECT_BY_STATUS = s"SELECT * FROM $TABLE_NAME WHERE STATUS='FINISHED'"
  override def init:Boolean={
    if(exist){
      configs=getList(SELECT_ALL)().map(conf => (conf.id, conf)).toMap
      true
    }
    else
      execute(CREATE_TABLE)()
  }
  init

  override def from(entity:Knowledge):Array[Any]={
    Array(entity.name, entity.datasource, entity.collector, entity.parser, entity.lasttime, entity.createtime, entity.id)
  }

  override def where(entity:Knowledge):String={
    val name:String=entity.name match{
      case None=>
        ""
      case Some(n:String)=>
        s"name like '%$n%'"
    }
    if(name=="" ){
      ""
    }
    else{
      s" where $name"
    }
  }

  override def entry(set:ResultSet):Knowledge={
    Knowledge(set.getString("ID"),
      Option(set.getString("NAME")),
      Option(set.getString("DRIVERID")),
      Option(set.getString("COLLECTORID")),
      Option(set.getString("PARSERID")),
      Option(set.getString("STATUS")),
      Option(set.getTimestamp("LAST_TIME")),
      Option(set.getTimestamp("CREATE_TIME")),
      set.getInt("IS_SAMPLE")
    )
  }
  protected def getList(sql: String)(array: Array[Any] = Array()): List[Knowledge] ={
    var lks=List[Knowledge]()
    val ks=query(sql)(array)
    while(ks.hasNext){
      lks=ks.next()::lks
    }
    lks
  }

  override def all(): List[Knowledge] = {
    var lks=List[Knowledge]()
    val ks= query(SELECT_ALL)(Array())
    while(ks.hasNext){
      lks=ks.next()::lks
    }
    lks
  }
  def queryByStatus(): List[Knowledge] = getList(SELECT_BY_STATUS)(Array())


  override def set(entity: Knowledge): Boolean = {
    entity.id match{
      case id:String =>
        if(exist(SELECT_BY_ID)(Array(entity.id)))
          execute(UPDATE)(Array(entity.name.orNull,  entity.datasource.orNull, entity.collector.orNull, entity.parser.orNull,entity.status.orNull, new Date(), id))
        else
          execute(INSERT)(Array(entity.name.orNull,  entity.datasource.orNull, entity.collector.orNull, entity.parser.orNull,Status.NOT_EXEC.toString, id))
      case "" =>
        execute(INSERT)(Array(entity.name.orNull,  entity.datasource.orNull, entity.collector.orNull, entity.parser.orNull,entity.status.orNull, UUID.randomUUID().toString))
    }
    init
  }

  override def delete(id: String): Boolean = {
    //if (execute(DELETE)(Array(id))) true else false
    //删除知识库表中的记录的同时，删除对应的知识库表
    val tbname = "KNOWLEDGE_"+id.replace("-","_").toUpperCase()
  //derby  val num = count("select COUNT(*) from SYS.SYSTABLES where  TABLETYPE = 'T' AND  TABLENAME='"+tbname+"'")()
    val num = count("select COUNT(*) from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='"+tbname+"'")()
    if (num != 0){
      val sql = """drop table """"+tbname+"""""""
      if(execute(sql)()) {
       execute(DELETE)(Array(id))
      } else
        false
    }else
     execute(DELETE)(Array(id))
  }

  def getColumns(table:String):Array[String]={
    val statement: PreparedStatement = from(s"SELECT * FROM $table where 1=2", Array())
    val set: ResultSet = statement.executeQuery()
    val meta = set.getMetaData
    val rt = (1 to meta.getColumnCount)
      .map { i =>
        meta.getColumnName(i).toUpperCase
      }.toArray
    set.close()
    statement.close()
    connection.commit()
    rt
  }

  def byCollector(id:String):List[Knowledge]={
    init
    configs.values.filter(conf=>conf.collector.contains(id)).toList
  }

  def byParser(id:String):List[Knowledge]={
    init
    configs.values.filter(conf=>conf.parser.contains(id)).toList
  }

  def byDataSource(id:String):List[Knowledge]={
    init
    configs.values.filter(conf => conf.datasource.contains(id)).toList
  }
}
