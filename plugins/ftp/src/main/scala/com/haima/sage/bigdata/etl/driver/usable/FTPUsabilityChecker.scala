package com.haima.sage.bigdata.etl.driver.usable

import com.haima.sage.bigdata.etl.common.model.{Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.{FTPDriver, FTPMate}

import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class FTPUsabilityChecker(mate: FTPMate) extends UsabilityChecker {
  val driver = FTPDriver(mate)
  val msg = mate.uri + "error:"

  override def check: Usability = {
    driver.driver() match {
      case Success(client) =>
        client.disconnect()
        Usability()
      case Failure(e) =>
        Usability(usable = false, cause = msg + e.getMessage)

    }
  }
}
