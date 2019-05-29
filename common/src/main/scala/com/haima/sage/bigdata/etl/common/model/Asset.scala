package com.haima.sage.bigdata.etl.common.model

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.JsonIgnore

/**
  * Created: 2016-05-24 19:34.
  * Author:zhhuiyan
  * Created: 2016-05-24 19:34.
  *
  *
  */
case class Asset(id: String = UUID.randomUUID().toString,
                 name: String,
                 ipAddress: String,
                 //securityDomain: SecurityDomain,
                 macAddress: Option[String],
                 mobile: Option[String],
                 responsible: Option[String],
                 desci: Option[String],
                 assetsTypeId: String,
                 assetsType: String = null,
                 @JsonIgnore
                 lasttime: Option[Date] = Some(new Date()),
                 @JsonIgnore
                 createtime: Option[Date] = Some(new Date())) extends LastAndCreateTime