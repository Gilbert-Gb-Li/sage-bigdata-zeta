package com.haima.sage.bigdata.etl.monitor.file

import java.io.{File, InputStream}
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.FileWrapper
import com.haima.sage.bigdata.etl.utils.FileUtils
import org.apache.hadoop.fs._

case class HDFSFileWrapper(fs: FileSystem, path: Path, parent: String, _encoding: Option[String]) extends FileWrapper[HDFSFileWrapper] {
  private val split: String = File.separator
  protected val utils: FileUtils = SeekableFileUtils

  override def exists(): Boolean = {
    try {
      val b = fs.exists(path) && !name.endsWith("_COPYING_")
      logger.debug(s"check  path($absolutePath) exists:$b")
      b
    } catch {
      case e: Exception =>
        logger.error(s" check file status has error:$e ")
        false
    }

  }

  override def name: String = path.getName

  override def listFiles(): Iterator[HDFSFileWrapper] = {
    try {
      val iterator = fs.listStatus(path)
      // fs.listStatus(path).foreach(status=>logger.debug(s"status next:::${status.getPath}"))
      val list: Iterator[HDFSFileWrapper] = iterator.map(next =>{
          val paths = next.getPath.toString.split("/")
          HDFSFileWrapper(fs,new Path(s"/${paths.slice(3, paths.length).mkString("/")}"), absolutePath, _encoding)
        }).filter(file => !file.name.endsWith("_COPYING_")).toIterator
      list
    } catch {
      case e: Exception =>
        logger.error(s" check file status has error:$e ")
        throw e
    }
  }

  override def isDirectory: Boolean = {
      fs.isDirectory(path)
  }

  override def absolutePath: String = {
    if (parent.endsWith(split)) {
      parent + name
    } else {
      parent + split + name
    }

  }


  /*  override def stream: BufferedReader = {
      try {
        strm = fs.open(path,FILE_READER_THREAD_BUFFER)
        strm
      } catch {
        case e: Exception =>
          null
      }

    }*/

  override def lastModifiedTime: Long = {
    val status = fs.getFileStatus(path)
    status.getModificationTime + status.getLen
  }

  override def uri: String = path.toString


  override lazy val inputStream: InputStream = {
    val stream:InputStream=try{
     fs.open(path)
    }catch{
      case e: Throwable=> {
        e.printStackTrace()
        null
      }
    }
    stream
  }

  override lazy val metadata: Map[String, Any] = {
    val status = fs.getFileStatus(path)
    Map("accessTime" -> new Date(status.getAccessTime),
      "size" -> status.getLen,
      "modificationTime" -> new Date(status.getModificationTime))
  }
}