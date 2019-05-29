package com.haima.sage.bigdata.etl.common.model

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


/**
  * Created: 2016-06-02 11:28.
  * Author:zhhuiyan
  * Created: 2016-06-02 11:28.
  *
  *
  */
@JsonIgnoreProperties(Array("lasttime", "createtime"))
case class DataSourceWrapper(id: String = UUID.randomUUID().toString,
                             name: String,
                             data: DataSource = null,
                             collector: Option[String] = None,
                             lasttime: Option[Date] = Some(new Date()),
                             createtime: Option[Date] = Some(new Date()),
                             isSample: Int = 0) extends LastAndCreateTime

object DataSourceStatus extends Enumeration {
  type DataSourceStatus = Value
  val INIT, RUNNING, STOP = Value
}