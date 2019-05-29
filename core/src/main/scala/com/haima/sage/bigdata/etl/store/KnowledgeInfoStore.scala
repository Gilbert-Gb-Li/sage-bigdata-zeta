package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet

import com.haima.sage.bigdata.etl.common.model.Pagination
import com.haima.sage.bigdata.etl.utils.Mapper

import scala.collection.mutable

/**
  * Created by liyju on 2017/11/20.
  */
class KnowledgeInfoStore extends DBStore[mutable.Map[String, String],String] with Mapper{
  def TABLE_NAME(tableName:String): String = tableName

  def SELECT_COUNT(tableName:String) = s"SELECT COUNT(*) FROM ${TABLE_NAME(tableName)}"

  def SELECT_ALL(tableName:String) = s"SELECT * FROM ${TABLE_NAME(tableName)}"

  def CHECK_TABLE(tableName:String) = s"SELECT * FROM $tableName OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY"
  override protected def CREATE_TABLE: String = ""

  override protected def INSERT: String = ""

  override protected def UPDATE: String = ""

  override protected def entry(set: ResultSet): mutable.Map[String, String] = {
    val hm =mutable.Map[String, String]()
    val metaData = set.getMetaData
    val count = metaData.getColumnCount
    var i = 1
    while ( {
      i <= count
    }) {
      val key = metaData.getColumnLabel(i)
      val value = set.getString(i)
      hm.put(key, value)

      {
        i += 1; i - 1
      }
    }
    hm
  }

  override protected def from(entity: mutable.Map[String, String]): Array[Any] = {
    Array[Any]()
  }

   def queryInfoByPage(start: Int, limit: Int, orderBy: Option[String], order: Option[String], sample: Option[String]): Pagination[mutable.Map[String, String]] = {
    val tableName = sample match {
      case Some(knowledgeId) if knowledgeId.isInstanceOf[String] =>
       // where(tableName.asInstanceOf[String])
        TABLE_NAME("\"KNOWLEDGE_" + knowledgeId.replace("-", "_").toUpperCase()+"\"")
      case _ =>
        ""
    }
     if("".equals(tableName)){ //判断是否串了参数
       Pagination[mutable.Map[String, String]]()
     }else{
       //判断是否存在表
       if(exist(CHECK_TABLE(tableName))(Array())){
         val total = count(SELECT_COUNT(tableName) + "")(Array())
         val fetch = s" OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
         val resultList = query(SELECT_ALL(tableName) + "" + fetch)(Array(start, limit)).toList
         Pagination[mutable.Map[String, String]](start, limit, total, result=resultList)
       }else{
         Pagination[mutable.Map[String, String]]()
       }

     }

  }

  override protected def where(entity: mutable.Map[String, String]): String = ""

  override protected def TABLE_NAME: String = ""
}
