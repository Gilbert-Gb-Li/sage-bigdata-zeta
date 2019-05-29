package com.haima.sage.bigdata.etl.driver.usable

import com.haima.sage.bigdata.etl.common.model.{JDBCSource, JDBCWriter, Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.{DriverMate, JDBCDriver, JDBCMate}

import scala.util.{Failure, Success, Try}

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class FowardUsabilityChecker(mate: DriverMate) extends UsabilityChecker {
  val msg: String = mate.uri + " has error:"
  override def check: Usability = {
    Usability()
  }

}
