package com.haima.sage.bigdata.etl.common.model.writer

/**
  * Created by liyju on 2018/1/10.
  */
case class SyslogOutputFormat (priorityType:Option[String]=Some("DATA")
                         ,priority:Option[String]=None
                         ,facility:Option[String]=None
                         ,level:Option[String]=None
                         ,timestampType:Option[String]=Some("DATA")
                         ,timestamp:Option[String]=None
                         ,hostname:Option[String]=None
                         ,tag:Option[String]=None){

}
