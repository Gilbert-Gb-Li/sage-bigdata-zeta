package com.haima.sage.bigdata.etl.common.model

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}

/**
  * Created by zhhuiyan on 2017/3/23.
  * 新增属性
  * 修改表结构
  */

@JsonIgnoreProperties(Array("lasttime", "createtime"))
case class TaskWrapper(id: Option[String] = Some(UUID.randomUUID().toString),
                       name: String,
                       config: String =null,
                       data: Task,
                       @JsonIgnore
                        lasttime: Option[Date] = Some(new Date()),
                       @JsonIgnore
                        createtime: Option[Date] = Some(new Date()),
                       isSample: Int = 0
                       ) extends LastAndCreateTime