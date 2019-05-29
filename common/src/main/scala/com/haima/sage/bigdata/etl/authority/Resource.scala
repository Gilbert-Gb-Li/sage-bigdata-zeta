package com.haima.sage.bigdata.etl.authority

/**
  * Created by zhhuiyan on 2016/9/29.
  */
sealed class Resource(val name: String) extends Serializable

object Resource {
  val DATA_SOURCE = new Resource("datasource")

  val PARSE_RULE = new Resource("parse")

  val ASSET = new Resource("asset")

  val ASSET_TYPE = new Resource("assettype")

  val WRITER = new Resource("writer")

  val DICTIONARY = new Resource("dictionary")

  val USER = new Resource("user")

  val ROLE = new Resource("role")

  val GROUP = new Resource("group")
}

