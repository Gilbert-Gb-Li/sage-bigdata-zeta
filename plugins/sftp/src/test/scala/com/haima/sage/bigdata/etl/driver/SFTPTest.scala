package com.haima.sage.bigdata.etl.driver

import java.io.File
import java.util
import java.util.Properties

import com.jcraft.jsch.{Channel, ChannelSftp, JSch, Session}
import com.haima.sage.bigdata.etl.common.model.{FileSource, SFTPSource}
import org.junit.Test

/**
  * Created by zhhuiyan on 2016/3/21.
  */
class SFTPTest {

  @Test
  def test(): Unit = {
    val jsch: JSch = new JSch()

    val session = jsch.getSession("bigdata", "10.10.100.58", 22)
    val config = new Properties()
    config.setProperty("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session.setPassword("bigdata")
    session.setTimeout(60000)
    session.connect()

    val channel = session.openChannel("sftp")
    channel.connect()

    val sftp = channel.asInstanceOf[ChannelSftp]
    import scala.collection.JavaConversions._
    val list = sftp.ls("/home/bigdata/sftp/backup").asInstanceOf[util.Vector[ChannelSftp#LsEntry]]
    list.foreach(e => {
      println(e.getFilename + "   isDir = " + e.getAttrs.isDir + "   isLink = " + e.getAttrs.isLink)
    }
    )

    val jsch2: JSch = new JSch()
    val session2 = jsch2.getSession("bigdata", "10.10.100.58", 22)
    val config2 = new Properties()
    config2.setProperty("StrictHostKeyChecking", "no")
    session2.setConfig(config)
    session2.setPassword("bigdata")
    session2.setTimeout(60000)
    session2.connect()

    val channel2 = session.openChannel("sftp")
    channel2.connect()

    val sftp2 = channel.asInstanceOf[ChannelSftp]
    import scala.collection.JavaConversions._
    val list2 = sftp2.ls("/home/bigdata/sftp/backup").asInstanceOf[util.Vector[ChannelSftp#LsEntry]]
    list2.foreach(e => {
      println(e.getFilename + "   isDir2 = " + e.getAttrs.isDir + "   isLink2 = " + e.getAttrs.isLink)
    }
    )

    try {
      sftp.ls("/home/bigdata/sftp/backup").asInstanceOf[util.Vector[ChannelSftp#LsEntry]]
    } catch {
      case e:Exception =>
        e.printStackTrace()
    }


    sftp.disconnect()

    try {
      sftp.ls("/home/bigdata/sftp/backup").asInstanceOf[util.Vector[ChannelSftp#LsEntry]]
    } catch {
      case e:Exception =>
        e.printStackTrace()
    }
    sftp2.disconnect()

    println("end")
  }

  val split: String = File.separator
  def nameWithPath(path: String) = {
    val paths = path.split("[\\\\/]")
    // logger.debug(s"----${paths.mkString(",")}")
    if (paths.length == 1) {
      (path, "." + split)
    } else if (paths.length == 2) {
      (split+paths(paths.length - 1), split)
    } else {
      (paths(paths.length - 1), paths.slice(0, paths.length - 1).mkString("/"))
    }
  }

  @Test
  def nameWithPathTest(): Unit ={
    var d = nameWithPath("/data/")

    println(d._1.replaceAll("\\" + File.separator,""))
    println(nameWithPath("/data/test.txt"))
    println(nameWithPath("/data/test1/test1.txt"))
    println(nameWithPath("/data/test1/"))
    println(File.separator)
  }


  @Test
  def config(): Unit ={
    val source =SFTPSource("127.0.1",Option(22),FileSource("/opt/a"),None)
  }

  @Test
  @throws[Exception]
  def test2() {
    val jsch: JSch = new JSch
    val session: Session = jsch.getSession("root", "172.16.219.76", 22)
    val config: Properties = new Properties
    config.setProperty("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session.setPassword("raysdata@2014")
    session.setTimeout(60000)
    session.connect
    val channel: Channel = session.openChannel("sftp")
    channel.connect
    val sftp: ChannelSftp = channel.asInstanceOf[ChannelSftp]
    sftp.ls("/opt/")
    import scala.collection.JavaConversions._
    sftp.ls("/opt/").foreach(obj=>{
      obj.asInstanceOf[ChannelSftp#LsEntry]
    })

    sftp.disconnect
  }

}
