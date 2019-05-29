package com.haima.sage.bigdata.etl.monitor.file


import java.io._
import java.util.Date

import com.haima.sage.bigdata.etl.common.exception.LogProcessorException
import com.haima.sage.bigdata.etl.common.model.FileWrapper
import com.haima.sage.bigdata.etl.stream.FTPInputStream
import com.haima.sage.bigdata.etl.utils.FileUtils
import org.apache.commons.net.ftp.{FTPClient, FTPFile, FTPReply}

case class FTPFileWrapper(fileSystem: FTPClient, properties: Map[String, String], root: String, path: String, parent: String, _encoding: Option[String]) extends FileWrapper[FTPFileWrapper] {
  lazy val fs: FTPClient = fileSystem
  private val split: String = "/"
  override val name: String = path
  override val absolutePath: String = if (parent.endsWith(split)) {
    parent + name
  } else {
    parent + split + name
  }
  lazy val file: Option[FTPFile] = {
    if (parent == "") {
      val ftpFile = fs.listFiles("./")
      ftpFile.find(_.getName == name)
    } else {
      // logger.debug(s"exists parent:$parent,pwd:${fs.printWorkingDirectory()}")
      if (parent == "/" && path == "") {
        val root = fs.mlistFile(parent)
        Option(root)
      } else {
        logger.debug(s"parent:$parent,name:${name}")
        val ftpFile = fs.listFiles(parent)

//        val length = ftpFile.length
//        var bool = true
//        var f :FTPFile = null
//        var i = 0
//        while(bool){
//          if(i==length){
//            bool = false
//            f = null
//          }else{
//            f = ftpFile(i)
//            if(f.getName.equals(name)){
//              bool = false
//            }
//          }
//          i+=1
//        }
        val f = ftpFile.find(_.getName.equals(name))
        f
      }
    }
  }

  override def exists(): Boolean = {
    file match {
      case None =>
        false
      case null =>
        false
      case Some(v) =>
        true
    }
  }

  def getFtpClient(): FTPClient = {
    val client = new FTPClient()
    client.setDataTimeout(60000)
    client.connect(properties.get("host").get, properties.get("port").get.toInt)
    client.enterRemotePassiveMode()
    client.login(properties.get("user").get, properties.get("password").get)
    val reply = client.getReplyCode
    if (!FTPReply.isPositiveCompletion(reply)) {
      client.disconnect()
      logger.error(s"FTP server refused connection.")
      throw new LogProcessorException("FTP server refused connection. ")
    }
    client
  }

  override def listFiles(): Iterator[FTPFileWrapper] = {
    logger.debug(absolutePath)
    val files = fs.listNames(absolutePath).map {
      sub =>
        FTPFileWrapper(fs, properties, root, if (sub.contains(absolutePath)) {
          sub.replaceFirst(absolutePath + (if (absolutePath.endsWith("/")) "" else "/"), "")
        } else {
          sub
        }, absolutePath, _encoding)
    }.toIterator
    files
  }


  /*{
      val iterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)
      for (x: LocatedFileStatus <- iterator.next()) yield HDFSFileWrapper(fs, x.getPath)
    }*/

  override def isDirectory: Boolean = {
    //logger.debug(s" isDirectory name : $name,parent:$parent,curent:${fs.printWorkingDirectory()}")
    file match {
      case None =>
        false
      case Some(v) =>
        if (v.isUnknown || v.isSymbolicLink) {
          fs.changeWorkingDirectory(absolutePath)
          if (fs.getReplyCode == 550) {
            false
          } else {
            fs.changeWorkingDirectory(absolutePath.split("/")(0))
            true
          }
        } else {
          v.isDirectory
        }
    }
  }


  override lazy val inputStream: InputStream = {
    logger.debug(s" stream parent : $parent ,path: ${fs.printWorkingDirectory()},absolutePath:$absolutePath")
    val ftpClient = this.getFtpClient
    val s = ftpClient.retrieveFileStream(new String(absolutePath.getBytes("UTF-8"), "ISO-8859-1"))
    FTPInputStream(ftpClient, s)
  }

  override def lastModifiedTime: Long = {
    file match {
      case None =>
        -1l
      case Some(v) =>
        v.getTimestamp.getTimeInMillis
    }
  }

  override def uri: String = root + "/" + absolutePath

  override protected val utils: FileUtils = JavaFileUtils

  override def close(): Unit = {

    super.close()
//    logger.debug("fs closed")
    //fs.disconnect()

  }

  override lazy val metadata: Map[String, Any] = {
    file match {
      case None =>
        Map()
      case Some(v) =>
        Map("timestamp" -> new Date(v.getTimestamp.getTimeInMillis),
          "size" -> v.getSize)
    }
  }
}