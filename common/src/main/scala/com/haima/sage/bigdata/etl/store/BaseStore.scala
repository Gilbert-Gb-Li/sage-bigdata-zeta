package com.haima.sage.bigdata.etl.store

import com.haima.sage.bigdata.etl.common.model.Pagination

/**
  * Created: 2016-05-26 15:51.
  * Author:zhhuiyan
  * Created: 2016-05-26 15:51.
  *
  *
  */
trait BaseStore[T, ID] {

  def metadata():Map[String,String]
  def set(assetType: T): Boolean

  def delete(id: ID): Boolean

  def get(id: ID): Option[T]

  def all(): List[T]

  def queryByPage(start: Int = 0, limit: Int = 20, orderBy: Option[String], order: Option[String], sample: Option[T]): Pagination[T]

}
