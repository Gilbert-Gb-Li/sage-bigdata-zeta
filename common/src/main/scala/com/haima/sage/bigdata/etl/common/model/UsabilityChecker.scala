package com.haima.sage.bigdata.etl.common.model

import com.haima.sage.bigdata.etl.driver.DriverMate

/**
  * Created: 2016-07-06 17:47.
  * Author:zhhuiyan
  * Created: 2016-07-06 17:47.
  *
  *
  */
trait UsabilityChecker {
  def mate: DriverMate

  protected def msg: String

  def check: Usability
}

case class Usability(id: String = null,
                     name: String = "",
                     usable: Boolean = true,
                     cause: String = "")
