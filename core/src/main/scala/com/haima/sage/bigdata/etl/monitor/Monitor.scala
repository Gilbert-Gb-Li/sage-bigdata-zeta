package com.haima.sage.bigdata.etl.monitor

import java.util

import akka.actor.{Actor, ActorSelection}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.utils.Logger

import scala.util.Try


/**
  * Created by zhhuiyan on 15/1/28.
  */


trait Monitor extends Actor with AutoCloseable with Logger {
  lazy val positionServer: ActorSelection = context.actorSelection("/user/position")

  var isRunning = false


  val pool: Int = CONF.getInt(PROCESS_SIZE) match {
    case 0 =>
      PROCESS_SIZE_DEFAULT
    case a =>
      a
  }
  val WAIT_TIME: Long = Try(Constants.CONF.getLong(PROCESS_WAIT_TIMES)).getOrElse(PROCESS_WAIT_TIMES_DEFAULT)


  def send(reader: Option[LogReader[_]], parser: Option[Parser[MapRule]]): Unit = {

    reader match {
      case Some(_) if context != null && context.parent != null =>

        context.parent ! (reader, parser)
        if (!isRunning) {
          context.parent ! (ProcessModel.MONITOR, Status.RUNNING)
          isRunning = true
        }

      case Some(r) =>
        r.close()
        logger.warn(s"maybe self is closing, ignore")
      case _ =>
        logger.warn(s"not find real reader, ignore")

    }
  }

  val processed: util.Queue[String] = new util.concurrent.LinkedBlockingQueue[String]()

  //val execute:Runnable
  def run(): Unit

  override def receive: Receive = {
    case STOP =>
      this.close()
      logger.info(s"monitor[${self.path.name}]  closed")

      context.stop(self)

      sender() ! (ProcessModel.MONITOR, Status.CLOSED)

    case START =>
      context.parent ! (ProcessModel.MONITOR, Status.RUNNING)
      this.run()

    case (Opt.DELETE, path: String) =>
      logger.info(s"monitor remove path[$path]")
      processed.remove(path)
    case _ =>
  }
}
