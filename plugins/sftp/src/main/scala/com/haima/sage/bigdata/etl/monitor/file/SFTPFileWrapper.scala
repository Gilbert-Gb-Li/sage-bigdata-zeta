package com.haima.sage.bigdata.etl.monitor.file

import java.io.{File, InputStream}
import java.util.{Date, Properties}

import com.jcraft.jsch.{ChannelSftp, JSch, Session, SftpException}
import com.haima.sage.bigdata.etl.common.exception.LogProcessorException
import com.haima.sage.bigdata.etl.common.model.{FileWrapper}
import com.haima.sage.bigdata.etl.utils.FileUtils

import scala.util.{Failure, Success, Try}

case class SFTPFileWrapper(fileSystem: ChannelSftp, properties: Map[String, String], root: String, path: String, parent: String, _encoding: Option[String]) extends FileWrapper[SFTPFileWrapper] {
  lazy val fs: ChannelSftp = fileSystem
  private val split: String = File.separator
  override val name: String = path
  override val absolutePath: String = if (parent.endsWith(split)) {
    (parent + name.replaceAll("\\" + split, "")).replaceAll("\\" + split, "/")
  } else {
    (parent + split + name.replaceAll("\\" + split, "")).replaceAll("\\" + split, "/")
  }


  lazy val entry: Option[ChannelSftp#LsEntry] = {
    import scala.collection.JavaConversions._
    if (parent == "") {
      val vector = fs.ls("./")
      vector.asInstanceOf[java.util.Vector[ChannelSftp#LsEntry]].find(_.getFilename == name)
    } else if ("\\".equals(parent)) {
      val vector = fs.ls("/")
      vector.asInstanceOf[java.util.Vector[ChannelSftp#LsEntry]].find(
        _.getFilename == (if (name.startsWith(File.separator)) name.replaceAll("\\" + File.separator, "") else name)
      )
    } else {
      try {
        //此处让线程短暂暂停100毫秒，确保ChannelSftp客户端链接关闭完成。
        Thread.sleep(100)
        if(fs.isClosed) {
          None
        } else {
          val vector = fs.ls(parent)
          vector.asInstanceOf[java.util.Vector[ChannelSftp#LsEntry]].find(_.getFilename == name)
        }
      } catch {
        case e: SftpException =>
          logger.error("fs is None", e)
          None
        case e: Exception =>
          logger.error(s"the path($parent) is't exist in $root : ", e)
          None
      }
    }
  }

  override def exists(): Boolean = {
    entry match {
      case None => false
      case Some(_) => true
    }
  }

  def getSftpClient(): ChannelSftp = {
    val client: Try[Session] = {
      Try {
        val jsch: JSch = new JSch()
        val session = jsch.getSession(properties.get("user").get, properties.get("host").get, properties.get("port").get.toInt)
        val config = new Properties()
        config.setProperty("StrictHostKeyChecking", "no")
        session.setConfig(config)
        val pwd = properties.get("password").get
        if (pwd != null && !pwd.equals(""))
          session.setPassword(pwd)
        session.setTimeout(6000)
        session.connect()
        session
      }
    }
    client match {
      case Success(fs) =>
        val channel = fs.openChannel("sftp")
        channel.connect()
        channel.asInstanceOf[ChannelSftp]
      case Failure(e) =>
        throw new LogProcessorException("SFTP server refused connection. ")
    }
  }

  override def listFiles(): Iterator[SFTPFileWrapper] = {
    logger.debug(s"absolutePath is $absolutePath")
    import scala.collection.JavaConversions._
    val entries = try {
      val vector = fs.ls(absolutePath)
      vector.asInstanceOf[java.util.Vector[ChannelSftp#LsEntry]]
    } catch {
      case e: SftpException =>
        logger.error("fs is null", e)
        new java.util.Vector()
      case e: Exception =>
        logger.error("listFiles error", e)
        new java.util.Vector()
    }
    val files = entries.filterNot(entry => ".".equals(entry.getFilename) || "..".equals(entry.getFilename))
      .map {
        sub =>
          SFTPFileWrapper(fs, properties, root,
            sub.getFilename
            , absolutePath, _encoding)
      }.toIterator
    files
  }


  /*{
      val iterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)
      for (x: LocatedFileStatus <- iterator.next()) yield HDFSFileWrapper(fs, x.getPath)
    }*/

  override def isDirectory: Boolean = {
    entry match {
      case None =>
        false
      case Some(v) =>
        v.getAttrs.isDir || v.getAttrs.isLink
    }
  }


  override lazy val inputStream: InputStream = {
    logger.debug(s" stream parent : $parent ,absolutePath:$absolutePath")
    val sftpClient = this.getSftpClient()
    sftpClient.get(absolutePath)
  }

  override def lastModifiedTime: Long = {
    entry match {
      case None =>
        -1l
      case Some(v) =>
        v.getAttrs.getMTime
    }
  }

  override def uri: String = absolutePath

  override protected val utils: FileUtils = JavaFileUtils

  override def close(): Unit = {

    logger.debug("sftp is close")
    super.close()
    /*fs.disconnect()
    fs.exit()*/
  }

  override lazy val metadata: Map[String, Any] = {
    entry match {
      case None =>
        Map()
      case Some(v) =>
        val attrs = v.getAttrs
        Map("accessTime" -> new Date(attrs.getATime),
          "modificationTime" -> new Date(attrs.getMTime),
          "size" -> attrs.getSize)
    }
  }
}