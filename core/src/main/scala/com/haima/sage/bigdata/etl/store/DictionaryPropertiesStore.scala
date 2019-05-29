package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.DictionaryProperties

/**
  * Created: 2016-05-26 15:26.
  * Author:zhhuiyan
  * Created: 2016-05-26 15:26.
  *
  *
  */
class DictionaryPropertiesStore extends  DBStore[DictionaryProperties, String]{

  override val TABLE_NAME = "DICTIONARY_PROPERTY"
  val CREATE_TABLE = s"CREATE TABLE $TABLE_NAME(id VARCHAR(64) PRIMARY KEY NOT NULL, dictionaryId VARCHAR(64), " +
    s"name VARCHAR(64), value VARCHAR(64), orderIndex INTEGER)"
  override val INSERT = s"INSERT INTO $TABLE_NAME(id, dictionaryId, name, value, orderIndex) VALUES (?, ?, ?, ?, ?)"
  override val UPDATE = s"UPDATE $TABLE_NAME SET name=?, value=?, orderIndex=? WHERE id=?"
  override val SELECT_BY_ID = s"SELECT * FROM $TABLE_NAME WHERE id=?"
  override val SELECT_ALL = s"SELECT * FROM $TABLE_NAME"

 private lazy val dictionaryStore: DictionaryStore =Stores.dictionaryStore
  init
  override def from(entity: DictionaryProperties): Array[Any] = Array(entity.key,
    entity.value, entity.vtype, entity.lasttime, entity.createtime)

  override def where(entity: DictionaryProperties): String = {
    ""
  }
  override def entry(resultSet: ResultSet) = {
    val dictionary = dictionaryStore.get(resultSet.getString("dictionaryId")).get
    DictionaryProperties(
      resultSet.getString("name"),
      resultSet.getString("value"),
      resultSet.getString("vtype"),
      resultSet.getTimestamp("lasttime"),
      resultSet.getTimestamp("createtime")
    )
  }
}