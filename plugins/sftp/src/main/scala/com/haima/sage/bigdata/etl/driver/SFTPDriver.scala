package com.haima.sage.bigdata.etl.driver

import java.util.Properties

import com.jcraft.jsch.{JSch, Session}
import com.haima.sage.bigdata.etl.common.model.WithProperties
import com.haima.sage.bigdata.etl.utils.Logger

import scala.util.Try

/**
  * Created by zhhuiyan on 2016/11/22.
  */
case class SFTPDriver(mate: SFTPMate) extends WithProperties with Driver[Session] with Logger {

  def driver(): Try[Session] = {
    val user = get("user").getOrElse("")
    val password = get("password", "")
    val jsch: JSch = new JSch()
    Try {
      val session = jsch.getSession(user, mate.host, mate.port.getOrElse(22))
      val config = new Properties()
      config.setProperty("StrictHostKeyChecking", "no")
      session.setConfig(config)
      if (password != null && !password.equals(""))
        session.setPassword(password)
      session.setTimeout(6000)
      session.connect()
      session
    }
  }

  override def properties: Option[Properties] = mate.properties
}
