package com.haima.sage.bigdata.etl.monitor

import akka.actor.ActorSystem
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule


class NetMonitor(conf: NetSource, parser: Parser[MapRule]) extends Monitor {

  def withActorSystem(name: String): Option[LogReader[_]] = {
    val clazz: Class[LogReader[_]] = Class.forName(name).asInstanceOf[Class[LogReader[_]]]
    Some(clazz.getConstructor(classOf[NetSource], classOf[ActorSystem]).newInstance(conf, context.system))

  }

  def reader(): Option[LogReader[_]] = {
    try {
      val name: String = Constants.READERS.getString(conf.protocol.get.toString)
      logger.debug(s"${conf.protocol.get}")
      logger.debug(s"$name")


      if (name != null) {

        println(conf.protocol)
        conf.protocol match {
          case Some(_: Akka | _: Net ) =>
            withActorSystem(name: String)
          case _ =>
            val clazz: Class[LogReader[_]] = Class.forName(name).asInstanceOf[Class[LogReader[_]]]
            Some(clazz.getConstructor(classOf[NetSource]).newInstance(conf))
        }

      } else {
        throw new UnsupportedOperationException("unknown type for NetMonitor.")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(s" something is wrong:" + e)
        throw e
    }

  }

  @throws[Exception]
  override def run(): Unit = {

    try {
      send(reader(), Some(parser))
    } catch {
      case e: Exception =>
        logger.error("create reader error:{}", e)
        context.parent ! (ProcessModel.MONITOR, Status.ERROR,s"MONITOR_ERROR:${e.getMessage}")
        context.stop(self)
    }

  }

  override def close(): Unit = {

  }


}
