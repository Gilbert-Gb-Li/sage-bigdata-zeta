package com.haima.sage.bigdata.etl.knowledge

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.haima.sage.bigdata.etl.common.model.LastAndCreateTime


@JsonIgnoreProperties(Array("lasttime", "createtime"))
//知识库接口
case class Knowledge(id: String = UUID.randomUUID().toString,
                     //知识库名称
                     name: Option[String] = None,
                     //数据源
                     datasource: Option[String] = None,
                     //采集器
                     collector: Option[String] = None,
                     //解析器
                     parser: Option[String] = None,
                     //状态
                     status: Option[String] = None,
                     @JsonIgnore
                     lasttime: Option[Date] = Some(new Date()),
                     @JsonIgnore
                     createtime: Option[Date] = Some(new Date()),
                     isSample: Int = 0
                     )extends LastAndCreateTime
