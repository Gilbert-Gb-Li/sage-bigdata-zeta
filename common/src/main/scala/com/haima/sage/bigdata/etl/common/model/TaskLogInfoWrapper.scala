package com.haima.sage.bigdata.etl.common.model

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.haima.sage.bigdata.etl.common.model.Status.Status

/**
  * Created by liyju on 2018/3/21.
  */

@JsonIgnoreProperties(Array("lasttime", "createtime"))
case class TaskLogInfoWrapper(id: String = UUID.randomUUID().toString,
                              taskId: String,
                              taskType: Option[String],
                              configId:Option[String],
                              action:Option[String],
                              msg:Option[String]=Some(""),
                              taskStatus:Option[String] = Some("SUCCESS"),
                              configStatus:Option[String]=Some("STOPPED"),
                              @JsonIgnore
                           lasttime: Option[Date] = Some(new Date()),
                              @JsonIgnore
                           createtime: Option[Date] = Some(new Date()),
                              isSample: Int = 0
                           ) extends LastAndCreateTime