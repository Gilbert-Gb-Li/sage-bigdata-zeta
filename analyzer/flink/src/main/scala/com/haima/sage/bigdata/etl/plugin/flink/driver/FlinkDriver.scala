package com.haima.sage.bigdata.etl.plugin.flink.driver

import java.util.UUID

import akka.actor.{ActorContext, ActorRef, Props}
import com.haima.sage.bigdata.etl.driver.{Driver, FlinkMate}
import com.haima.sage.bigdata.etl.plugin.flink.FlinkWatcher
import com.haima.sage.bigdata.etl.utils.Logger

import scala.util.Try

class FlinkDriver(override val mate: FlinkMate)(implicit val context: ActorContext)
  extends Driver[ActorRef] with Logger {

  logger.debug(s"flink driver to " + mate.uri)

  override def driver(): Try[ActorRef] =
    Try(context.actorOf(Props.create(classOf[FlinkWatcher], mate.uri), s"flink-driver-${mate.uri.replaceAll("/", "") + UUID.randomUUID().getLeastSignificantBits}"))
}
