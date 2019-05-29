package com.haima.sage.bigdata.etl.common.model

import java.sql.Timestamp
import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * Created: 2016-05-16 19:32.
  * Author:zhhuiyan
  * Created: 2016-05-16 19:32.
  *
  *
  */
@JsonIgnoreProperties(Array("lasttime", "createtime"))
case class AssetType(id: Option[String] = Some(UUID.randomUUID().toString),
                     name: String = null,
                     parentId: Option[String] = None, //父节点ID
                     lasttime: Option[Date] = Some(new Timestamp(System.currentTimeMillis())),
                     createtime: Option[Date] = Some(new Timestamp(System.currentTimeMillis()))) extends LastAndCreateTime