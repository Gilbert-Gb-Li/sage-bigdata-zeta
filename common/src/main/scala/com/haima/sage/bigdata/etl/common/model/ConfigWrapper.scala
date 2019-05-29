package com.haima.sage.bigdata.etl.common.model

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.haima.sage.bigdata.etl.common.model.Status.Status

case class ConfigWrapper(id: String = UUID.randomUUID().toString,
                         name: String,
                         /*数据源*/
                         datasource: Option[String] = None,
                         /*解析规则*/
                         parser: Option[String] = None,
                         /*数据输出*/
                         writers: List[String] = List(),

                         `type`: Option[ChannelType] = Some(ParserChannel()),
                         /*数据通道的状态*/
                         @JsonScalaEnumeration(classOf[StatusType])
                         status: Status = Status.PENDING,
                         /*错误信息*/
                         errorMsg: Option[String] = None,
                         /*数据通道所在的机器*/
                         collector: Option[String] = None,
                         /*数据通道的公共配置信息*/
                         properties: Option[Map[String, String]] = None,
                         @JsonIgnore
                         lasttime: Option[Date] = Some(new Date()),
                         @JsonIgnore
                         createtime: Option[Date] = Some(new Date()),
                         /*是否初始化数据*/
                         isSample: Int = 0) extends LastAndCreateTime