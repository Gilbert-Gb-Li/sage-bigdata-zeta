package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet

import com.haima.sage.bigdata.etl.common.model.{Dictionary, DictionaryProperties}

/**
  * Created: 2016-05-26 15:26.
  * Author:zhhuiyan
  * Created: 2016-05-26 15:26.
  *
  *
  */
 class DictionaryStore extends DBStore[Dictionary, String] {

  override val TABLE_NAME = "DICTIONARY"
  val CREATE_TABLE: String = s"CREATE TABLE $TABLE_NAME(id VARCHAR(64) PRIMARY KEY NOT NULL, " +
    s"name VARCHAR(64), vkey VARCHAR(64), properties CLOB, lasttime TIMESTAMP, createtime TIMESTAMP)"
  override val INSERT = s"INSERT INTO $TABLE_NAME(id, name, vkey, properties, lasttime, createtime) VALUES (?, ?, ?, ?, ?, ?)"
  override val UPDATE = s"UPDATE $TABLE_NAME SET name=?, vkey=?, properties=?, lasttime=? WHERE id=?"
  override val SELECT_BY_ID = s"SELECT * FROM $TABLE_NAME WHERE id=?"

  override val SELECT_ALL = s"SELECT * FROM $TABLE_NAME"

  init
  override def from(entity: Dictionary): Array[Any] = Array(entity.name, entity.key, entity.lasttime, entity.createtime, entity.id)

  override def where(entity: Dictionary): String = {
    ""
  }

  override def entry(resultSet: ResultSet) = {

    Dictionary(resultSet.getString("id"),
      resultSet.getString("name"),
      resultSet.getString("vkey"),
      mapper.readValue[List[DictionaryProperties]](resultSet.getString("properties")),
      resultSet.getTimestamp("lasttime"),
      resultSet.getTimestamp("createtime"))
  }

  override def set(dictionary: Dictionary) = {
    if (exist(SELECT_BY_ID)(Array(dictionary.id)))
      execute(UPDATE)(Array(dictionary.name, dictionary.key,
        mapper.writeValueAsString(dictionary.properties), dictionary.lasttime, dictionary.id))
    else execute(INSERT)(Array(dictionary.id, dictionary.name, dictionary.key,
      mapper.writeValueAsString(dictionary.properties), dictionary.lasttime, dictionary.createtime))
  }
}