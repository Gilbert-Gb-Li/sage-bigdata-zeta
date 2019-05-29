package com.haima.sage.bigdata.analyzer.sql.side

import java.util.Collections

import com.dtstack.flink.sql.side.cache.{AbsSideCache, CacheObj, LRUSideCache}
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.types.Row


trait AsyncReqRow extends RichAsyncFunction[Row, Row] with ReqRow {

  @transient
  var sideCache: AbsSideCache = _

  def getFromCache(key: String): CacheObj = {
    sideCache.getFromCache(key)
  }

  def putCache(key: String, obj: CacheObj): Unit = {
    sideCache.putCache(key, obj)
  }

  def openCache(): Boolean = {
    sideCache != null
  }

  def dealMissKey(input: Row, resultFuture: ResultFuture[Row]): Unit = {
    // 区分Join类型 Left or other
    resultFuture.complete(Collections.singleton(fillData(input, null)))
  }

  override def initCache(): Unit = {
    sideCache.initCache()
  }
}
