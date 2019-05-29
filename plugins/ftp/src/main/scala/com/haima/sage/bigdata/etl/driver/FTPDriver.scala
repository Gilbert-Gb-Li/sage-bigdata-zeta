package com.haima.sage.bigdata.etl.driver

import java.util.Properties

import com.haima.sage.bigdata.etl.common.exception.LogProcessorException
import com.haima.sage.bigdata.etl.common.model.{FTPSource, WithProperties}
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.commons.net.ftp.{FTPClient, FTPReply}

import scala.util.Try

/**
  * Created by zhhuiyan on 2016/11/22.
  */
case class FTPDriver(mate: FTPMate) extends WithProperties with Driver[FTPClient] with Logger {
  override def driver(): Try[FTPClient] = Try {
    val user: String = get("user", "")
    val password: String = get("password", "")
    val client = new FTPClient()
    client.setControlEncoding("UTF-8")
    client.setDataTimeout(60000)
    client.connect(mate.host, mate.port.getOrElse(21))

    mate match {
      case d: FTPSource =>
        //服务器被动模式
        //client.enterRemotePassiveMode()
        client.enterRemotePassiveMode()
      case _ =>
        //本地被动模式
        client.enterLocalPassiveMode()
    }
    if (!(user == null || user == "" || password == null || password == ""))
      client.login(user, password)
    else
      client.login("anonymous", "")
    val reply = client.getReplyCode
    if (!FTPReply.isPositiveCompletion(reply)) {
      client.disconnect()
      logger.error(s"FTP server refused connection.")
      throw new LogProcessorException("FTP server refused connection. ")
    }
    client
  }



  override def properties: Option[Properties] = mate.properties
}
