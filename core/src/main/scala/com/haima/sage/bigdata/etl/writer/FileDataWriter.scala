package com.haima.sage.bigdata.etl.writer

import java.io.{BufferedWriter, File}

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.Opt.STOP
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.writer.NameFormatter
import com.haima.sage.bigdata.etl.metrics.MeterReport

import scala.concurrent.duration._


/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */

class FileDataWriter(conf: FileWriter, report: MeterReport) extends DefaultWriter[FileWriter](conf, report: MeterReport) with BatchProcess {
  private final val mapper = Formatter(conf.contentType)

  private final val formatter = NameFormatter(conf.path, conf.persisRef)
  private var existQueues: Map[String, BufferedWriter] = Map()

  // 是否停止重连：true 停止/ false 继续重连
  private var stopConnect = false
  // 重连执行器
  private var cancellable: akka.actor.Cancellable = null

  def get(t: Map[String, Any]): BufferedWriter = {
    val path: String = formatter.format(t)
    existQueues.get(path) match {
      case Some(w) =>
        w.newLine()
        w
      case _ =>
        /* 声明file对象 */
        val file = new File(path)
        /* 判断文件是否存在，并捕获异常信息，出现异常则停止采集器 */
        try {
          /* 判断文件是否存在，不存在则创建文件 */
          if (!file.exists) {
            /* 判断文件上级目录是否存在，不存在则创建目录 */
            if (!file.getParentFile.exists()) {
              file.getParentFile.mkdirs()
            }
            file.createNewFile()
          }
        } catch {
          case e: Exception => {
            logger.error(s"Check output file path[$path] error", e)
            connect = false
            context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, s"file ${conf.path} does not exist!")
          }
        }
        if (connect) {
          /* 判断file是否为文件，并且是否有写入权限，否则报出一场信息并停止采集器 */
          if (!file.isFile || !file.canWrite) {
            connect = false
            context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, s"your set ${conf.path} can't write able!")
            null
          } else {
            val w = file.bufferedWriter(true)

            if (file.length > 0) {
              w.newLine()
            }
            existQueues = existQueues + (path -> w)
            w
          }
        } else null
    }
  }

  override def write(t:RichMap): Unit = {
    if (getCached - cacheSize == 0) {
      flush()
    }

    if (t != null) {
      val writer = get(t)
      val line = mapper.string(t)
      if (writer != null) {
        try {
          writer.append(line)
        } catch {
          case e: Exception =>
            logger.error("", e)
        }
      } else {
        //        flush()
        //        existQueues.values.foreach(_.close())
        if (!stopConnect) {
          self ! ("reconnect", t)
        }
      }
    } else {
      logger.warn("some data null")
    }
  }

  override def close(): Unit = {
    flush()
    existQueues.values.foreach(_.close())
    context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
    context.stop(self)
  }

  override def flush(): Unit = {
    existQueues.values.foreach(_.flush())
    report()
  }

  override def redo(): Unit = {

  }

  // 停止重连方法
  def stopReconnect(): Unit = {
    if (cancellable != null) {
      logger.info("Stop reconnect")
      cancellable.cancel()
      cancellable = null
      stopConnect = false
    }
  }

  override def receive: Receive = {
    case ("reconnect", t: Map[String@unchecked, Any@unchecked]) =>
      import context.dispatcher
      cancellable = context.system.scheduler.schedule(1 seconds, 3 seconds)({
        connect = true
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.RUNNING, "Reconnecting Writer")
        logger.info(s"[${self.path.name}] start reconnect. stopConnect = $stopConnect")
        if (get(t) != null) {
          stopReconnect()
          write(t)
        }
      })
    case STOP =>
      logger.info(s"Stopping ${self.path.name}")
      stopReconnect()
      super.receive(STOP)
    case other =>
      super.receive(other)
  }

}