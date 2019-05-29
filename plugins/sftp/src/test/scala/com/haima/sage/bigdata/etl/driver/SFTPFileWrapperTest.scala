package com.haima.sage.bigdata.etl.driver

import java.util.Properties

import com.jcraft.jsch.{ChannelSftp, Session}
import com.haima.sage.bigdata.etl.codec.DelimitCodec
import com.haima.sage.bigdata.etl.common.model.{FileSource, ProcessFrom, SFTPSource, TxtFileType}
import com.haima.sage.bigdata.etl.monitor.file.SFTPFileWrapper
import org.junit.Test

import scala.util.{Failure, Success, Try}

/**
  * Created by bbtru on 2017/9/28.
  */
class SFTPFileWrapperTest {

  //如果assert断言失败会抛 java.lang.AssertionError: assertion failed
  val host = "10.10.100.58"
  val port = Some(22)
  val path = "/home/bigdata/sftp/backup"
  val skip = 0
  val user = "bigdata"
  val password = "bigdata"

  /**
    * 测试构造SFTPFileWrapper类功能
    */
  @Test
  def testSftpFileWrapper(): Unit = {
    //FileSource
    val fileSource = new FileSource(path, Some("other"),
      Some(new TxtFileType()), None, Some(new DelimitCodec("delimit")), Some(ProcessFrom.END), skip)
    //Properties
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    //SFTPSource
    val sftpSource = new SFTPSource(host, port, fileSource, Some(properties))
    val session: Try[Session] = SFTPDriver(sftpSource).driver()
    session match {
      case Success(s) =>
        val channel = s.openChannel("sftp")
        channel.connect()
        val sftp = channel.asInstanceOf[ChannelSftp]
        val root = "sftp://10.10.100.58:Some(22)//home/bigdata/sftp/backup"
        /*var sftpFileWrapper = new SFTPFileWrapper(sftp, root, "backup", "/home/bigdata/sftp", None)
        val listFiles = sftpFileWrapper.listFiles()
        while(listFiles.hasNext) {
          val item = listFiles.next()
          assert(item.isDirectory==false)
        }*/
      case Failure(e) =>
        println("SFTP server can't connection.")
        e.printStackTrace()
    }
  }

  /**
    * 测试以下功能列表：
    * (1)是否是目录
    * (2)获取文件信息 访问时间、修改时间、文件大小
    */
  @Test
  def testFunction(): Unit = {
    //FileSource
    val fileSource = new FileSource(path, Some("other"),
      Some(new TxtFileType()), None, Some(new DelimitCodec("delimit")), Some(ProcessFrom.END), skip)
    //Properties
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    //SFTPSource
    val sftpSource = new SFTPSource(host, port, fileSource, Some(properties))
    val session: Try[Session] = SFTPDriver(sftpSource).driver()
    session match {
      case Success(s) =>
        val channel = s.openChannel("sftp")
        channel.connect()
        val sftp = channel.asInstanceOf[ChannelSftp]
        val root = "sftp://10.10.100.58:Some(22)//home/bigdata/sftp/backup"
        /*val sftpFileWrapper = new SFTPFileWrapper(sftp, root, "backup", "/home/bigdata/sftp", None)
        val listFiles = sftpFileWrapper.listFiles()
        while(listFiles.hasNext) {
          val item = listFiles.next()
          val metaData = item.metadata
          println("目录状态 = "+item.isDirectory)
          println("Meta信息 = "+metaData.toString())
        }*/
      case Failure(e) =>
        println("SFTP server can't connection.")
        e.printStackTrace()
    }
  }

}
