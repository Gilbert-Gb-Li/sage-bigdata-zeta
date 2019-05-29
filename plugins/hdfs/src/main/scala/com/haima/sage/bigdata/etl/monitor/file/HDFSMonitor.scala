package com.haima.sage.bigdata.etl.monitor.file

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.driver.{HDFSDriver}
import com.haima.sage.bigdata.etl.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration._
import scala.util.{Failure, Success}


class HDFSMonitor(conf: HDFSSource, override val parser: Parser[MapRule]) extends Listener[HDFSFileWrapper] {
  override val source = conf.path
  implicit val timeout = Timeout(5 days)
  private val fs: FileSystem =
    HDFSDriver(conf).driver() match {
      case Success(fileSystem) =>
        fileSystem
      case Failure(e) =>
        context.parent ! (ProcessModel.MONITOR, Status.ERROR, "MONITOR_ERROR:hdfs  refused connection. please check you conf ")
        throw new Exception(s"hdfs init error :cause:$e")

    }

  override def close(): Unit = {
    super.close()
    TimeUnit.SECONDS.sleep(10)
    fs.close()
  }

  override def getFileWrapper(path: String): HDFSFileWrapper = {
    val tuple = nameWithPath(path)
    HDFSFileWrapper(fs, new Path(path), tuple._2, source.encoding)
  }

  override protected val utils: FileUtils = SeekableFileUtils


}