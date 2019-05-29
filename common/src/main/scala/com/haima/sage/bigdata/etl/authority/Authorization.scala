package com.haima.sage.bigdata.etl.authority

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

/**
  * Created by zhhuiyan on 15/5/5.
  */
@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[Authorization], name = "show"),
  new Type(value = classOf[Authorization], name = "create"),
  new Type(value = classOf[Authorization], name = "delete"),
  new Type(value = classOf[Authorization], name = "upload"),
  new Type(value = classOf[Authorization], name = "download"),
  new Type(value = classOf[Authorization], name = "update"),
  new Type(value = classOf[Start], name = "start"),
  new Type(value = classOf[Stop], name = "stop")

))
sealed class Authorization(val name: String) extends Serializable

case class Start(config: String) extends Authorization("start")

case class Stop(config: String) extends Authorization("stop")

object Authorization {
  val SHOW = new Authorization("show")

  val CREATE = new Authorization("create")

  val UPDATE = new Authorization("update")

  val DELETE = new Authorization("delete")

  val UPLOAD = new Authorization("upload")

  val DOWNLOAD = new Authorization("download")
}

