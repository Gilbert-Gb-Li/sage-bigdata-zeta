package com.haima.sage.bigdata.etl.common.model

/**
 * Created by zhhuiyan on 15/3/10.
 */
case class Event(header:Option[Map[String,Any]]=None, content: String)
