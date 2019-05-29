package com.haima.sage.bigdata.etl.monitor.file

import com.jcraft.jsch.{ChannelSftp, Session}
import com.haima.sage.bigdata.etl.common.exception.LogProcessorException
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model.{Parser, ProcessModel, SFTPSource, Status}
import com.haima.sage.bigdata.etl.driver.SFTPDriver
import com.haima.sage.bigdata.etl.utils.FileUtils

import scala.util.{Failure, Success, Try}


class SFTPMonitor(conf: SFTPSource, override val parser: Parser[MapRule]) extends Listener[SFTPFileWrapper] {
  override val source = conf.path
  val user: String = conf.get("user", null)
  val password: String = conf.get("password", null)

  var client: Try[Session] = SFTPDriver(conf).driver()

  private val fileSystem: ChannelSftp = {
    client match {
      case Success(fs) =>
        val channel = fs.openChannel("sftp")
        channel.connect()
        channel.asInstanceOf[ChannelSftp]
      case Failure(e) =>
        context.parent ! (ProcessModel.MONITOR, Status.ERROR, "MONITOR_ERROR:SFTP server refused connection. maybe user/password incorrect ")
        throw new LogProcessorException("SFTP server refused connection. ")
    }
  }


  override def getFileWrapper(path: String): SFTPFileWrapper = {
    val tuple = nameWithPath(path)
    val host = conf.host.toString
    val port = conf.port.get.toString
    val user = conf.properties.get.getProperty("user")
    val password = conf.properties.get.getProperty("password")
    val properties = Map("host" -> host, "port" -> port, "user" -> user, "password" -> password)
    SFTPFileWrapper(fileSystem, properties, conf.uri, tuple._1, tuple._2, source.encoding)
  }

  override protected val utils: FileUtils = JavaFileUtils

  override def close(): Unit = {
    super.close()
    fileSystem.disconnect()
    client.foreach(_.disconnect())
  }
}