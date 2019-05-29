package com.haima.sage.bigdata.etl.common.model

import java.util.Date

trait LastAndCreateTime extends Serializable {
  def lasttime: Option[Date]
  def createtime: Option[Date]
}
