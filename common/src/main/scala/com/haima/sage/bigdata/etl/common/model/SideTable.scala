package com.haima.sage.bigdata.etl.common.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}


@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[AllSideTable], name = "all"),
  new Type(value = classOf[AsyncSideTable], name = "async")
))
trait SideTable {
  val name: String = getClass.getSimpleName.replace("SideTable", "").toLowerCase()
}

case class AllSideTable(
                         // 维表更新时间（秒）
                         updateTime: Long = 60
                       ) extends SideTable

case class AsyncSideTable(
                           // 缓存大小，条数
                           cacheSize: Long = 100000,
                           // 未命中缓存失效时间（秒）
                           expireTime: Long = 60
                         ) extends SideTable