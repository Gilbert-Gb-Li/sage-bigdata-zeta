package com.haima.sage.bigdata.etl.common.model

import java.util.Date

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.haima.sage.bigdata.etl.common.model.MetricPhase.MetricPhase
import com.haima.sage.bigdata.etl.common.model.MetricType.MetricType

case class MetricInfo(collectorId: String,
                      metricId: String,
                      configId: String,
                      @JsonScalaEnumeration(classOf[MetricTypeRef])
                      metricType: MetricType,
                      @JsonScalaEnumeration(classOf[MetricPhaseRef])
                      metricPhase: MetricPhase,
                      metric: Map[String, Any],
                      @JsonIgnore
                      lasttime: Option[Date] = Some(new Date()),
                      @JsonIgnore
                      createtime: Option[Date] = Some(new Date()))
  extends LastAndCreateTime

case class MetricHistory(metricId: String,
                         configId: String,
                         historyType: String,
                         count: Long,
                         @JsonIgnore
                         lasttime: Option[Date] = Some(new Date()),
                         @JsonIgnore
                         createtime: Option[Date] = Some(new Date())) extends LastAndCreateTime

class MetricTypeRef extends TypeReference[MetricType.type]

case class MetricWrapper(
                          info: List[MetricInfo],
                          count: Map[String, Long]
                        )

object MetricType extends Enumeration {
  type MetricType = Value
  val IN, SUCCESS, FAIL, IGNORE = Value
}

class MetricPhaseRef extends TypeReference[MetricPhase.type]

object MetricPhase extends Enumeration {
  type MetricPhase = Value
  val READ, OPERATION, WRITE, FLINKIN, FLINKOUT = Value
}

class MetricTableRef extends TypeReference[MetricTable.type]

object MetricTable extends Enumeration {
  type MetricTable = Value
  val METRIC_INFO_SECOND = Value("METRIC_INFO_SECOND")
  val METRIC_INFO_MINUTE = Value("METRIC_INFO_MINUTE")
  val METRIC_INFO_HOUR = Value("METRIC_INFO_HOUR")
  val METRIC_INFO_DAY = Value("METRIC_INFO_DAY")
}