package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable
import java.util.Date

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.haima.sage.bigdata.etl.common.model.Status.Status


/**
  * Created by evan on 17-8-4.
  */
@JsonIgnoreProperties(Array("lasttime", "createtime"))
case class ModelingWrapper(
                            id: Option[String],
                            name: String,
                            `type`: Option[AnalyzerChannel],
                            /*数据通道*/
                            channel: String,
                            /*分析规则*/
                            analyzer: Option[String] = None,
                            /*数据输出*/
                            sinks: List[String] = List(),
                            /*状态*/
                            @JsonScalaEnumeration(classOf[StatusType])
                            status: Status = Status.PENDING,
                            /*错误信息*/
                            errorMsg: Option[String] = None,
                            /*数据通道所在的机器*/
                            worker: Option[String] = None,
                            /*数据通道的公共配置信息*/
                            properties: Option[Map[String, String]] = None,
                            @JsonIgnore
                            lasttime: Option[Date] = Some(new Date()),
                            @JsonIgnore
                            createtime: Option[Date] = Some(new Date()),
                            isSample: Int = 0
                          ) extends LastAndCreateTime

case class ModelingConfig(id: String,
                          channel: SingleChannel,
                          worker: Option[Collector] = None,
                          sinks: List[Writer] = List(),
                          properties: Option[Map[String, String]] = None
                         ) extends Serializable