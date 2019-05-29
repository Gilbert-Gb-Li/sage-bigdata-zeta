package com.haima.sage.bigdata.etl.driver.usable

import com.haima.sage.bigdata.etl.common.model.{Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.{HDFSDriver, HDFSMate}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class HDFSUsabilityChecker(mate: HDFSMate) extends UsabilityChecker {
  lazy val logger = LoggerFactory.getLogger(classOf[HDFSUsabilityChecker])
  val driver = HDFSDriver(mate)
  val msg = mate.uri + " error: "

  override def check: Usability = {
    driver.driver() match {
      case Success(fileSystem) =>
        Try(fileSystem.getStatus()) match {
          case Success(_) =>
            Try(fileSystem.close())
            Usability()
          case Failure(e) =>
            Try(fileSystem.close())
            logger.debug(s"get hdfs system status has error :"+e.getStackTrace.mkString(","))
            Usability(usable = false, cause = msg + e.getMessage)
        }
      case Failure(e) =>
        logger.debug(s"check hdfs has error :"+e.getStackTrace.mkString(","))
        Usability(usable = false, cause = msg + e.getMessage)

    }
  }
}
