package com.haima.sage.bigdata.etl.monitor.file

import com.haima.sage.bigdata.etl.common.exception.LogProcessorException
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model.{FTPSource, Parser, ProcessModel, Status}
import com.haima.sage.bigdata.etl.driver.FTPDriver
import com.haima.sage.bigdata.etl.utils.FileUtils
import org.apache.commons.net.ftp.FTPClient

import scala.util.{Failure, Success}


class FTPMonitor(conf: FTPSource, override val parser: Parser[MapRule]) extends Listener[FTPFileWrapper] {
  override val source = conf.path
  val user: String = conf.get("user", "")
  val password: String = conf.get("password", "")

  private val fileSystem: FTPClient =

    FTPDriver(conf).driver() match {
      case Success(fs) =>
        fs
      case Failure(e) =>
        context.parent ! (ProcessModel.MONITOR, Status.ERROR, s"MONITOR_ERROR:FTP server refused connection. maybe user/password incorrect ${e.getCause}")
        throw new LogProcessorException(s"FTP server refused connection. ${e.getMessage}")
    }


  override def getFileWrapper(path: String): FTPFileWrapper = {
    val tuple = nameWithPath(path)
    val host = conf.host.toString
    val port = conf.port.get.toString
    val user = conf.properties.get.getProperty("user")
    val password = conf.properties.get.getProperty("password")
    val properties = Map("host" -> host, "port" -> port, "user" -> user, "password" -> password)
   FTPFileWrapper(fileSystem, properties, conf.uri, tuple._1, tuple._2, source.encoding)
  }

  override protected val utils: FileUtils = JavaFileUtils

  override def close(): Unit = {
    fileSystem.disconnect()

    super.close()
  }
}