package com.haima.sage.bigdata.etl.writer

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.common.model.writer.NameFormatter
import com.haima.sage.bigdata.etl.driver.HDFSDriver
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.util.{Failure, Success}

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */

class HDFSDataWriter(conf: HDFSWriter, report: MeterReport) extends DefaultWriter[HDFSWriter](conf, report: MeterReport) with BatchProcess {

  private[HDFSDataWriter] val loader: ClassLoader = classOf[HDFSDataWriter].getClassLoader
  private final val mapper = Formatter(conf.path.contentType)

  private final val formatter = NameFormatter(conf.path.path, conf.path.persisRef)
  private var existQueues: Map[String, FSDataOutputStream] = Map()

  private val fs: FileSystem = HDFSDriver(conf).driver() match {
    case Success(f) =>
      f
    case Failure(e) =>
      throw e
  }

  def get(t: Map[String, Any]): FSDataOutputStream = {
    val path = formatter.format(t)
    existQueues.get(path) match {
      case Some(w) =>
        w
      case _ =>
        val file = new Path(path)
        val w =
          try {
            if (!fs.exists(file)) {
              fs.create(file)
            } else {
              if (!fs.isFile(file)) {
                connect = false
                context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, s"your set ${conf.path} can`t write able!")
                context.stop(self)
              }
              fs.append(file)
            }
          } catch {
            case e: Exception =>
              logger.error(s"hdfs  open file error:${e.getCause}")
              //context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, s"hdfs  open file error:${e.getCause}")
              flush()
              null
          }
        existQueues = existQueues + (path -> w)
        w

    }


  }


  override def write(t: RichMap): Unit = {

    if (getCached % cacheSize == 0) {
      flush()
    }
    if (t != null) {
      val writer = get(t)
      if(writer != null){
        writer.write(mapper.bytes(t))
        writer.write("\r\n".getBytes())
      }
    } else {
      logger.warn("hdfs writer:your want save data but get null")
    }

  }

  override def close(): Unit = {

    existQueues.values.foreach(w=>{
      if(w != null)
        w.close()
    })
    existQueues = Map()
    //fs.close()
    //context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
    //context.stop(self)
  }

  override def flush(): Unit = {

    try {
      //existQueues.values.foreach(_.getWrappedStream.asInstanceOf[DFSOutputStream].hsync(util.EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH)))
      //existQueues.values.foreach(_.hsync())
      close()
      report()
    } catch {
      case e: Exception =>
        logger.error(s"hdfs flush data error:${e.getCause}")
        //context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, s"hdfs flush data error:${e.getCause}")
        close()
    }

  }

  override def redo(): Unit = {

  }

}