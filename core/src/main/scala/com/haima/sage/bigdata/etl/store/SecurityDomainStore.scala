package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet

import com.haima.sage.bigdata.etl.common.model.SecurityDomain

/**
  * Created: 2016-05-24 19:02.
  * Author:zhhuiyan
  * Created: 2016-05-24 19:02.
  *
  *
  */
class SecurityDomainStore extends  DBStore[SecurityDomain, String] {

  override val TABLE_NAME = "SECURITY_DOMAIN"
  val CREATE_TABLE = s"CREATE TABLE $TABLE_NAME(id VARCHAR(64) PRIMARY KEY NOT NULL, name VARCHAR(64), orderIndex INTEGER)"

  override val INSERT = s"INSERT INTO $TABLE_NAME( name, orderIndex,id) VALUES (?, ?, ?)"
  override val UPDATE = s"UPDATE $TABLE_NAME SET name=?, orderIndex=? WHERE id=?"
  override val DELETE = s"DELETE FROM $TABLE_NAME WHERE id=?"

  init
  override def from(entity: SecurityDomain): Array[Any] = Array(entity.name, entity.orderIndex, entity.id)

  override def where(entity: SecurityDomain): String = {
    ""
  }


  override def entry(resultSet: ResultSet): SecurityDomain =
    SecurityDomain(resultSet.getString("id"), resultSet.getString("name"), resultSet.getInt("orderIndex"))

}