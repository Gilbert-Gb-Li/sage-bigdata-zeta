package com.haima.sage.bigdata.etl.driver

import java.util
import java.util.Properties

import com.jcraft.jsch.{ChannelSftp, Session}
import com.haima.sage.bigdata.etl.codec.DelimitCodec
import com.haima.sage.bigdata.etl.common.model.{FileSource, ProcessFrom, SFTPSource, TxtFileType}
import org.junit.Test

import scala.util.{Failure, Success, Try}

/**
  * Created by bbtru on 2017/9/28.
  */
class SFTPDriverTest {

  //如果assert断言失败会抛 java.lang.AssertionError: assertion failed
  val host = "10.10.100.58"
  val port = Some(21)
  val path = "/home/bigdata/sftp/backup"
  val skip = 0
  val user = "bigdata"
  val password = "bigdata"

  /**
    * 测试sftp驱动类是否可用
    */
  @Test
  def testSftpDriver(): Unit = {
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
        assert(sftp.isConnected == true)
        assert(sftp.isClosed == false)
      case Failure(e) =>
        println("SFTP server can't connection.")
        e.printStackTrace()
    }
  }

  /**
    * 测试ls()函数功能(显示目标列表)
    */
  @Test
  def testSftpLs(): Unit = {
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
        import scala.collection.JavaConversions._
        val list = sftp.ls(path).asInstanceOf[util.Vector[ChannelSftp#LsEntry]]
        list.foreach(e => {
          println("文件名 = "+e.getFilename+" 是否是目录 = "+e.getAttrs.isDir)
        })
        assert(list.size() == 3, s"该${path}里面有3个文件.")
      case Failure(e) =>
        println("SFTP server can't connection.")
        e.printStackTrace()
    }
  }

}
