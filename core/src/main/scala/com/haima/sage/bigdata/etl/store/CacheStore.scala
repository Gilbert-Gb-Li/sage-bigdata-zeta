package com.haima.sage.bigdata.etl.store

import java.sql.{ResultSet, SQLException}
import java.util.function.BiConsumer

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.Stream


/**
  * Created by zhhuiyan on 4/14/14.
  */

class CacheStore extends DBStore[Map[String, Any], String] {

  override def TABLE_NAME: String = "CACHE"

  val CREATE_TABLE: String = s"""CREATE TABLE $TABLE_NAME(
                                 |ID VARCHAR(200) PRIMARY KEY NOT NULL,
                                 |VALUE CLOB NOT NULL)""".stripMargin

  override val SELECT_BY_ID: String = "SELECT VALUE FROM CACHE WHERE ID=?"
  override val INSERT: String = "INSERT INTO CACHE(ID, VALUE) VALUES(?,?)"
  override val UPDATE: String = "UPDATE CACHE set  VALUE = ? where id =?"
  override val DELETE: String = "DELETE FROM CACHE WHERE ID=?"
  init
  override def entry(set: ResultSet): Map[String, Any] = {
    val d = set.getString("VALUE")
    try {

      val data = JSON.parseObject(d)
      import scala.collection.JavaConversions._
      data.toMap
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(s"re parse cache data error:${e.getCause}")
        Map("error" -> s"re parse cache data error:${e.getCause}", "raw" -> d)
    }

  }



  def toJava(log: Map[String, Any]): java.util.HashMap[String, Any] = {
    val map: java.util.HashMap[String, Any] = new java.util.HashMap()
    log.foreach {
      case (key, value: scala.collection.convert.Wrappers.MapWrapper[String@unchecked, Any@unchecked]) =>
        map.put(key, wrapper2Java(value))
      case (key, value) =>
        map.put(key, value)
    }
    map
  }

  def wrapper2Java(log: scala.collection.convert.Wrappers.MapWrapper[String, Any]): java.util.HashMap[String, Any] = {
    val map: java.util.HashMap[String, Any] = new java.util.HashMap()
    log.forEach(new BiConsumer[String, Any] {
      override def accept(t: String, u: Any): Unit = {
        u match {
          case value: scala.collection.convert.Wrappers.MutableMapWrapper[String@unchecked, Any@unchecked] =>
            map.put(t, wrapper2Java(value))
          case value: scala.collection.convert.Wrappers.MapWrapper[String@unchecked, Any@unchecked] =>
            map.put(t, wrapper2Java(value))
          case value =>
            map.put(t, value)
        }
      }
    })
    map
  }

  @throws[SQLException]
  def put(ID: String, log: Map[String, Any]): Boolean = {
    execute(INSERT)(Array(ID, JSON.toJSONString(toJava(log), SerializerFeature.WriteClassName)))

  }

  def getById(ID: String): Stream[Map[String, Any]] = {
    query(SELECT_BY_ID)(Array(ID))
  }

  def remove(ID: String): Boolean = {
    execute(DELETE)(Array(ID))

  }

  override def from(entity: Map[String, Any]): Array[Any] = Array(entity.toString())

  override def where(entity: Map[String, Any]): String = {
    ""
  }
}