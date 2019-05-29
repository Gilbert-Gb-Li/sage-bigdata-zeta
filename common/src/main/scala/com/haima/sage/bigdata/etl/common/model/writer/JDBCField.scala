package com.haima.sage.bigdata.etl.common.model.writer

/**
  * Created by liyju on 2018/1/11.
  */
case class JDBCField(name: String,
                     `type`: String,
                     pk: Option[String] = Some("N"),
                     nullable: Option[String] = Some("Y")
                     ,length:Option[Integer]=None,
                     decimal:Option[Integer]=None)
