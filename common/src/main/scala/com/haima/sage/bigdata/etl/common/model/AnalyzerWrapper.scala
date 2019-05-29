package com.haima.sage.bigdata.etl.common.model

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.fasterxml.jackson.core.`type`.TypeReference

class AnalyzerTypeTrf extends TypeReference[AnalyzerType.type]

object AnalyzerType extends Enumeration {
  type Type = Value
  final val MODEL = Value("model")
  final val ANALYZER = Value("analyzer")
}

/**
  * Created by zhhuiyan on 2017/5/16.
  */
@JsonIgnoreProperties(Array("lasttime", "createtime"))
case class AnalyzerWrapper(
                            id: Option[String] = Some(UUID.randomUUID().toString),
                            name: String,
                            data: Option[Analyzer],
                            /** 参考数据通道
                              */
                            channel: Option[String] = None,
                            @JsonIgnore
                            lasttime: Option[Date] = Some(new Date()),
                            @JsonIgnore
                            createtime: Option[Date] = Some(new Date()),
                            isSample: Int = 0
                          ) extends LastAndCreateTime
