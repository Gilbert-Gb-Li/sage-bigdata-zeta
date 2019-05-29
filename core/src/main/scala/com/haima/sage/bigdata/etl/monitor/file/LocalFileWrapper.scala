package com.haima.sage.bigdata.etl.monitor.file


import java.io._
import java.nio.file.{Files, Path}

import com.haima.sage.bigdata.etl.common.model.FileWrapper
import com.haima.sage.bigdata.etl.utils.FileUtils

case class LocalFileWrapper(path: Path, _encoding: Option[String]=None) extends FileWrapper[LocalFileWrapper] {


  protected val utils: FileUtils = JavaFileUtils

  override def exists(): Boolean = path match {
    case null =>
      false
    case f =>
      Files.exists(f)
  }

  override val name: String = path match {
    case null =>
      ""
    case f => f.toFile.getName
  }


  override def listFiles(): Iterator[LocalFileWrapper] = {
    import scala.collection.JavaConversions._
    val directory = Files.newDirectoryStream(path)
    val list = directory.map(path => LocalFileWrapper(path, _encoding)).filter(file =>
      !(file.name.endsWith(".DS_Store") || file.name.endsWith(".swp"))).toIterator
    directory.close()
    list
  }


  def procSubs(isRunning: () => Boolean, proc: (LocalFileWrapper) => Unit): Unit = {
    val stream = Files.newDirectoryStream(path)
    val directory = stream.iterator()
    while (isRunning() && directory.hasNext) {
      val file = LocalFileWrapper(directory.next(), _encoding)
      if (!(file.name.endsWith(".DS_Store") || file.name.endsWith(".swp"))) {
        proc(file)
      }

    }
    stream.close()
  }

  private lazy val isDir = path match {
    case null =>
      false
    case f =>
      Files.isDirectory(f)
  }

  override def isDirectory: Boolean = isDir

  override val absolutePath: String =
    path match {
      case null =>
        ""
      case f => f.toFile.getAbsolutePath
    }

  override lazy val lastModifiedTime: Long = if (exists()) {
    Files.getLastModifiedTime(path).toMillis
  } else {
    0l
  }


  def getParent: LocalFileWrapper = LocalFileWrapper(path.getParent, _encoding)

  override def uri: String = absolutePath


  private lazy val file = new RandomAccessFile(path.toFile, "r")

  override lazy val inputStream: InputStream = {

    //  Files.newInputStream(path)

    new FileInputStream(file.getFD)


  }


  override def skip(num: Long) = {
    if (file.length() < num) {
      file.length()
    } else {
      file.seek(num)
      num
    }
  }

  override lazy val metadata: Map[String, Any] = {
    import scala.collection.JavaConversions._
    Files.readAttributes(path, "*").toMap
      //Data Simple:
      //Map(lastModifiedTime -> 2017-07-03T02:45:33.074373Z,
      // isOther -> false,
      // isSymbolicLink -> false,
      // size -> 70179,
      // isRegularFile -> true,
      // isDirectory -> false,
      // fileKey -> null,
      // lastAccessTime -> 2017-07-03T02:45:06.768461Z,
      // creationTime -> 2017-07-03T02:45:06.768461Z)
      .filter(d => !d._1.startsWith("is") && d._2 != null) //过滤掉
      .map(data => (data._1, data._2.toString))


  }
}