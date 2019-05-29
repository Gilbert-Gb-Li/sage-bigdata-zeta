package com.haima.sage.bigdata.etl.common.model

import java.io._


/**
  * Created by zhhuiyan on 15/1/14.
  */


case class Config(id: String,
                  name:String = null,
                  channel: SingleChannel = null,
                  collector: Option[Collector] = None,
                  writers: List[Writer] = List(),
                  override val properties: Option[java.util.Properties] = None
                 ) extends Serializable with WithProperties
