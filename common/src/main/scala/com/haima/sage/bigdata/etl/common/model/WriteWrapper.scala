package com.haima.sage.bigdata.etl.common.model

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}

/**
  * Created: 2016-05-26 10:53.
  * Author:zhhuiyan
  * Created: 2016-05-26 10:53.
  *
  *
  */
@JsonIgnoreProperties(Array("lasttime", "createtime"))
case class WriteWrapper(id: Option[String] = Some(UUID.randomUUID().toString),
                        name: String,
                        writeType: Option[String],
                        data: Writer,
                        @JsonIgnore
                        lasttime: Option[Date] = Some(new Date()),
                        @JsonIgnore
                        createtime: Option[Date] = Some(new Date()),
                        isSample: Int = 0
                       ) extends LastAndCreateTime