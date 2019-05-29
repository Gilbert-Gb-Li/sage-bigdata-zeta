package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable
/**
  * Created by zhhuiyan on 2017/3/23.
  *
  */

case class Task(configId: String, enable: Boolean = false, taskName: Option[String]=null, cronExpression: String=null, jobType: Option[String]=Some("channel"),
                action: Option[String ] = Some("START"), retry:Integer = 3,
                retryInterval:Integer = 10) extends Serializable


