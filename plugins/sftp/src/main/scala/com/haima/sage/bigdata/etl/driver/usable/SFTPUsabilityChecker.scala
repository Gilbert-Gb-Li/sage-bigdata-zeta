package com.haima.sage.bigdata.etl.driver.usable

import com.haima.sage.bigdata.etl.common.model.{Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.{SFTPDriver, SFTPMate}

import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class SFTPUsabilityChecker(mate: SFTPMate) extends UsabilityChecker {
  val msg: String = mate.uri + "error:"

  override def check: Usability = {
    SFTPDriver(mate).driver() match {
      case Success(_) =>
        Usability()
      case Failure(e) =>
        Usability(usable = false, cause = msg + e.getMessage)

    }
  }
}
