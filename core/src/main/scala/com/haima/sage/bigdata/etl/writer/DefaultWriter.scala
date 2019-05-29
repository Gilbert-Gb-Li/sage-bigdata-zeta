package com.haima.sage.bigdata.etl.writer

import akka.actor.SupervisorStrategy.Stop
import akka.actor._

import com.haima.sage.bigdata.etl.common.base.LogWriter
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.Status._
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.metrics.MeterReport

import scala.concurrent.duration._


/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */

abstract class DefaultWriter[W <: Writer](conf: W, report: MeterReport) extends LogWriter[W] with Actor {

  import context.dispatcher
  // val cacheServer: ActorSelection = context.actorSelection("/user/cache")
  var stopping = false
 private var cached = 0


  def getCached=cached
  val cacheSize: Int = if (conf.cache > 0) conf.cache else 1000
  var connect = true

  // context.system.scheduler.schedule(FLUSH_TIME seconds, FLUSH_TIME seconds, self, Opt.FLUSH)

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(0) {
    case msg =>
      context.parent ! (self.path.name, ProcessModel.WRITER, ERROR, msg)
      Stop

  }


  override def redo(): Unit = {
    connect = true
  }

  def receive: Receive = {
    /*case (DONE, batches: Map[Long, Int]) =>
      tail(batches.values.sum)
      context.parent ! ("batches", batches)*/
    case CONNECTED =>
      connect = true
      context.parent ! (self.path.name, ProcessModel.WRITER, RUNNING)
      redo()
    case CONNECTING =>
      connect = false
      context.parent ! (self.path.name, ProcessModel.WRITER, PENDING, "CONNECTING")
    case LOST_CONNECTED =>
      connect = false
      context.parent ! (self.path.name, ProcessModel.WRITER, ERROR, "LOST_CONNECTED")

    case Terminated(_) =>
      connect = false
    //已经停止 不需要关闭了 context.stop(actor)
    case FLUSH =>
      flush()
    case STOP =>
      stopping = true
      flush()
      close()
    //cacheServer ! (self.path.name, Opt.STOP)
    case (_, STOP) =>
      flush()
      close()
    case (batch: Long, log: List[RichMap@unchecked]) =>
      process(batch, log)
    case msg =>
      logger.warn(s"sender:${sender()},unknown message:$msg")
  }

  private def process(batch: Long, log: List[RichMap]): Unit = {
    if (report.success.nonEmpty) report.success.get.mark(log.size)
    if (connect) {
      cached += 1
      write(batch: Long,log)
    } else {
      val send = sender()
      //todo bug fixed for unknown message
      context.system.scheduler.scheduleOnce(500 milliseconds)({
        self.tell((batch,log), send)
      })
    }
  }


  override def tail(num: Int): Unit = {
    if (cached > num) {
      cached -= num
    }
    else
      cached = 0
  }
}

