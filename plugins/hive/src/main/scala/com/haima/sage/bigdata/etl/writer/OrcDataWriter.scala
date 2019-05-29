package com.haima.sage.bigdata.etl.writer

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.writer.{NameFormatter, OrcType}
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.driver.HDFSDriver
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.orc.{OrcFile, Writer}

import scala.util.{Failure, Success}

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */

class OrcDataWriter(conf: HDFSWriter, report: MeterReport) extends DefaultWriter[HDFSWriter](conf, report: MeterReport) with BatchProcess {

  private[OrcDataWriter] val loader: ClassLoader = classOf[OrcDataWriter].getClassLoader

  import org.apache.orc.TypeDescription

  private final val formatter = NameFormatter(conf.path.path, conf.path.persisRef)
  lazy val config = new Configuration()
  lazy val fields = conf.contentType.get.asInstanceOf[OrcType]
  lazy val schema: TypeDescription = TypeDescription.fromString(fields.toString)

  import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch


  private var existQueues: Map[String, (Writer, VectorizedRowBatch)] = Map()

  private val fs: FileSystem = HDFSDriver(conf).driver() match {
    case Success(f) =>
      f
    case Failure(e) =>
      throw e
  }

  def get(t: Map[String, Any]): (Writer, VectorizedRowBatch) = {
    val path = formatter.format(t)
    existQueues.get(path) match {
      case Some(w) =>
        w
      case _ =>
        val file = new Path(path)
        val writer = OrcFile.createWriter(file,
          OrcFile.writerOptions(config).setSchema(schema))
        val batch: VectorizedRowBatch = schema.createRowBatch

        existQueues = existQueues + (path -> (writer, batch))
        (writer, batch)

    }


  }


  override def write(t: RichMap): Unit = {

    if (getCached % cacheSize == 0) {
      flush()
    }
    if (t != null) {
      val (_, batch) = get(t)
      fields.fields.zipWithIndex.foreach {
        case ((k, _), index) =>
          batch.cols(index).asInstanceOf[BytesColumnVector].vector(getCached) = t(k).toString.getBytes
      }

      batch.cols(getCached)


    } else {
      logger.warn("some data null")
    }

  }

  override def close(): Unit = {

    existQueues.values.foreach(_._1.close())
    fs.close()
    context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
    context.stop(self)
  }

  override def flush(): Unit = {

    try {
      existQueues.values.foreach(tuple => {
        tuple._1.addRowBatch(tuple._2)
        tuple._2.reset()
      })


      report()
    } catch {
      case e: Exception =>
        logger.warn(s"hdfs flush data error:${e.getCause}")
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, s"hdfs flush data error:${e.getCause}")
        close()
        throw e
    }

  }

  override def redo(): Unit = {

  }

}