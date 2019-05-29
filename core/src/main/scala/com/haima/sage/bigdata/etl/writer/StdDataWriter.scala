package com.haima.sage.bigdata.etl.writer

import akka.actor.ActorRef
import com.codahale.metrics.{Meter, Timer}
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.metrics.MeterReport

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */

class StdDataWriter(conf: StdWriter, timer: MeterReport) extends DefaultWriter[StdWriter](conf, timer: MeterReport) with BatchProcess {
  private final val mapper = Formatter(conf.contentType)

  override def write(t: RichMap): Unit = {

    if (getCached % cacheSize==0) {
      flush()
    }
    if (t != null) {
      println(mapper.string(t))
    } else {
      logger.warn("some data null")
    }

  }

  override def close(): Unit = {
    flush()
    context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
    context.stop(self)
  }

  override def flush(): Unit = {
    report()
  }

}