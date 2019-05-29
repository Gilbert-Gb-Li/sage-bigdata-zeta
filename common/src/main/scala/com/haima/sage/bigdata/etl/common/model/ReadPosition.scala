package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/31 17:16.
  */
case class ReadPosition(path: String, var records: Long, var position: Long, var finished: Boolean = false, modified: Long = 0) extends Serializable {
  def recordIncrement() {
    records += 1
  }

  def positionIncrement(pos: Long) {
    this.position += pos
  }

  def setRecords(records: Long) {
    this.records = records
  }


  def setPosition(position: Long) {
    this.position = position
  }
}



