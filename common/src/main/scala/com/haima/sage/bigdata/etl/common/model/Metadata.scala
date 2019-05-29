package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable

/**
  * Created by zhhuiyan on 15/4/9.
  */
case class Metadata(data: Option[List[(String, String, String)]] = None) extends Serializable


