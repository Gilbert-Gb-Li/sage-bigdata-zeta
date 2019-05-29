package com.haima.sage.bigdata.etl.writer

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.{ProcessModel, RestfulWriter, Status}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import com.haima.sage.bigdata.etl.util.HttpClientUtil


import scala.concurrent.duration._

/**
  * Created by liyju on 2017/8/30.
  */

@SuppressWarnings(Array("serial"))
class RestfulLogWriter(conf: RestfulWriter, report: MeterReport) extends DefaultWriter[RestfulWriter](conf, report: MeterReport) with BatchProcess {
  import  context.dispatcher
  private final val mapper = Formatter(conf.contentType)
  private lazy val clientUtil = new HttpClientUtil()
  private val url = conf.uri


  override def write(t: RichMap): Unit = {
    val data = mapper.string(t)
    try {
      val result = clientUtil.doPost(url, data, conf.contentType.map(_.encoding.getOrElse("utf-8")).getOrElse("utf-8"))
      logger.debug(s"restful get response： ${result}")
      if (!connect)
        connect = true
      if (getCached % cacheSize == 0)
        this.flush()
    } catch {
      case e: Exception =>
        logger.error(s"restful send error:${e.getMessage}")
        connect = false
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, s"restful send error:${e.getMessage}")
        context.system.scheduler.scheduleOnce(1 seconds, self, ("try", t))

    }
  }

  override def flush(): Unit = {
    report()
  }

  override def close(): Unit = {
    this.flush()
    context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
    context.stop(self)
  }

  override def redo(): Unit = {

  }

  override def receive: Receive = {
    case ("try", data: RichMap) =>
      write(data)
    case any =>
      super.receive(any)
  }
}
