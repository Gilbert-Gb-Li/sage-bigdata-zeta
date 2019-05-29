package com.haima.sage.bigdata.etl.common.model

import java.sql.Timestamp
import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.haima.sage.bigdata.etl.common.model.filter.MapRule


/**
  * Created: 2016-05-13 16:46.
  * Author:zhhuiyan
  * Created: 2016-05-13 16:46.
  *
  *
  */
@JsonIgnoreProperties(Array("lasttime", "createtime"))
case class ParserWrapper(id: Option[String] = Some(UUID.randomUUID().toString),
                         name: String,
                         sample: Option[String] = None,
                         parser: Option[Parser[MapRule]] = None,
                         datasource: Option[String] = None,
                         properties: Option[List[ParserProperties]] = Some(List()),
                         lasttime: Option[Date] = Some(new Timestamp(System.currentTimeMillis())),
                         createtime: Option[Date] = Some(new Timestamp(System.currentTimeMillis())),
                         isSample: Int = 0) extends LastAndCreateTime


case class ParserProperties(id: Option[String] = Some(UUID.randomUUID().toString),
                            name: Option[String] = None,
                            `type`: String,
                            key: String,
                            format: Option[String] = None,
                            sample: Option[String] = None
                           ) {
  override def hashCode(): Int = key.hashCode
}