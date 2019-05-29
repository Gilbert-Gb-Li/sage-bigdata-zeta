package com.haima.sage.bigdata.etl.checker

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.driver.NetMate
import com.haima.sage.bigdata.etl.utils.NetUtils
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by jdj on 2017/8/15.
  */
case class NetChecker(mate: NetMate) extends UsabilityChecker {

  protected val logger: Logger = LoggerFactory.getLogger(classOf[NetChecker])
  private val timeout = 50

  override protected def msg: String = mate.uri + " has error:"


  override def check: Usability = {
    if (mate.isClient) {
      NetUtils(mate.host.get, mate.port,mate.protocol.getOrElse(UDP())).connectAble match {
        case (true, _) =>
          Usability()
        case (_, msg) =>
          Usability(usable = false, cause = s"can not connect to remote[${mate.host.get + ":" + mate.port}] cause by  $msg".toString)
      }
    } else {
      NetUtils(mate.host.get, mate.port,mate.protocol.getOrElse(UDP())).bindAble match {
        case (true, _) =>
          Usability()
        case (_, msg) =>
          Usability(usable = false, cause = s"can not bind with [${mate.host.get + ":" + mate.port}] cause by $msg".toString)
      }
    }

  }
}


