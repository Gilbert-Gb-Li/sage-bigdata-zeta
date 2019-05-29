package com.haima.sage.bigdata.etl.common.model

/**
  * Created: 2016-05-26 15:53.
  * Author:zhhuiyan
  * Created: 2016-05-26 15:53.
  *
  *
  */

//case class Pagination[T] (start: Int = 0, limit: Int = 20,
//                         currentPage: Int = 0, totalPage: Int = 0, totalCount: Int = 0, result: List[T]) extends Serializable

case class Pagination[T](start: Int = 0, limit: Int = 20, totalCount: Int = 0, var currentPage: Int = 0, var totalPage: Int = 0, result: List[T] = List()) {

  totalPage = if (totalCount == 0) 0 else (totalCount + limit - 1) / limit

  val lastRow: Int = start + limit

  currentPage = (start + limit) / limit
}
