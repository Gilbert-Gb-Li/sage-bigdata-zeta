package com.haima.sage.bigdata.etl.common

import java.io._

import com.haima.sage.bigdata.etl.common.model.{RichFile, RichMap}

/**
  * Created by zhhuiyan on 2017/5/23.
  */
object Implicits {
  implicit def richFile(file: File): RichFile = {
    new RichFile(file)

  }

  implicit def richMap(data: Map[String, Any]): RichMap = {
    RichMap(data)

  }


}




