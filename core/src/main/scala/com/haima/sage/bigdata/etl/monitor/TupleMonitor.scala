package com.haima.sage.bigdata.etl.monitor

import akka.actor.Props
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule


class TupleMonitor(conf: TupleSource, parser: Parser[MapRule]) extends Monitor {


  private val first = {
    val clazz = Class.forName(MONITORS.getString(conf.first.name))

    context.actorOf(Props.create(clazz, conf.first, parser), s"monitor-first")
  }
  private var first_stream: LogReader[_] = _
  private var second_stream: LogReader[_] = _
  private val second = {
    val clazz = Class.forName(MONITORS.getString(conf.second.name))
    context.actorOf(Props.create(clazz, conf.second, parser), s"monitor-first")
  }

  def reader(): Option[LogReader[_]] = {
    /*try {
      val name: String = Contents.READERS.getString(conf.contentType)
      logger.debug(s"${conf.contentType}")
      logger.debug(s"$name")
      if (name != null) {
        val clazz: Class[LogReader[_]] = Class.forName(name).asInstanceOf[Class[LogReader[_]]]
        Some(clazz.getConstructor(classOf[NetSource]).newInstance(conf))
      } else {
        throw new UnsupportedOperationException("unknown type for NetMonitor.")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(s" something is wrong:" + e)
        throw e
    }*/
    null
  }

  @throws[Exception]
  override def run():Unit = {

    try {
      send(reader(), Some(parser))
    } catch {
      case e: Exception =>
        logger.error("create reader error:{}", e)
        context.parent ! (ProcessModel.MONITOR, Status.ERROR,s"MONITOR_ERROR:${e.getMessage}")
        context.stop(self)
    }

    }


  override def receive: Receive = {
    case (Some(reader: LogReader[_]), Some(_: Parser[_])) =>
      if (first_stream != null && sender().equals(first)) {
        first_stream = reader
      } else if (second_stream != null && sender().equals(second)) {
        second_stream = reader
      }
    case msg =>
      super.receive(msg)
  }


  override def close(): Unit = {

  }


}
