package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable

import com.fasterxml.jackson.core.`type`.TypeReference
import com.haima.sage.bigdata.etl.common.model.MetricPhase.MetricPhase
import com.haima.sage.bigdata.etl.common.model.MetricType.MetricType
import com.haima.sage.bigdata.etl.common.model.Status.Status


/**
  * Created by zhhuiyan on 15/1/22.
  */
class Message extends Enumeration {
  type Message = Value
}


object Status extends Message {
  type Status = Value

  val SUCCESS, FAIL, ERROR, READER_ERROR, WRITER_ERROR, LEXER_ERROR, WORKER_STOPPED, NOT_EXEC,
  PENDING, STARTING, RUNNING, CONNECTING, STOPPING, CLOSING, UNAVAILABLE, UNKNOWN,
  STOPPED, FINISHED, DONE, CLOSED, FLUSHED, CONNECTED, LOST_CONNECTED, MONITOR_ERROR = Value
}

class StatusType extends TypeReference[Status.type]

object Opt extends Message {
  type Opt = Value
  val WATCH, UNWATCH, INITIAL, REGISTER, UN_REGISTER, SUBSCRIBE, UN_SUBSCRIBE,
  SYNC, BROADCAST, PREVIEW, FORWARD, SEND,
  PUT, GET, FLUSH, REDO, CLOSE, CHECK, LOAD,
  START, RESTART,KILL, STOP, STOP_WHEN_FINISH,
  FLOW_CHECK, WAITING,
  CREATE, UPDATE, DELETE, RESET = Value

}


object ProcessModel extends Enumeration {
  type Model = Value
  final val PROCESSOR, EXECUTOR, LEXER, ANALYZER, STREAM, WRITER, MONITOR, KNOWLEDGE = Value

}

case class Result(status: String, message: String) extends Serializable

case class RunStatus(config: String,
                     path: Option[String] = None,
                     model: ProcessModel.Model,
                     value: Status,
                     errorMsg: Option[String] = None) extends Serializable

case class MetricQuery(configId: String, metricType: Option[MetricType], metricPhase: Option[MetricPhase], from: java.util.Date, to: java.util.Date)

case class BuildResult(status: String, key: String, param: String*) extends Serializable

class AnalyzerModelType extends TypeReference[AnalyzerModel.type]

object AnalyzerModel extends Enumeration {
  type AnalyzerModel = Value
  final val STREAMING = Value("streaming")
  final val MODELING = Value("modeling")
}
