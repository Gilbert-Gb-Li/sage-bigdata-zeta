package com.haima.sage.bigdata.etl.plugin.flink.driver.usable

import akka.actor.ActorContext
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.{FlinkAPIs, Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.FlinkMate
import com.haima.sage.bigdata.etl.plugin.flink.driver.FlinkDriver
import com.haima.sage.bigdata.etl.utils.{HTTPAddress, Logger}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class FlinkUsabilityChecker(override val mate: FlinkMate, implicit val context: ActorContext) extends UsabilityChecker with Logger {
  implicit val timeout = Timeout(10 seconds)

  override protected def msg: String = "flink"

  override def check: Usability = {

    new FlinkDriver(mate)(context).driver().map(flinkWatcher => {
      try {

        val overview = Await.result(flinkWatcher ? FlinkAPIs.CLUSTER_OVERVIEW, timeout.duration).asInstanceOf[Map[String, Any]]
        if (overview.contains("error")) {
          Usability(usable = false, cause = overview("error").toString)
        } else {

          HTTPAddress(mate.cluster) match {
            case Some(HTTPAddress(_, _, _, Some(url))) if url.contains("proxy") =>
              Usability()
            case _ =>
              /*val slots = overview("slots-available").asInstanceOf[Int]
              if (slots == 0) {
                //判断有没有可用资源
                Usability(usable = false, cause = s"flink[${mate.uri}] no slots available")
              } else {
                Usability()
              }*/
              if (overview.isEmpty) {
                //判断有没有可用资源
                Usability(usable = false, cause = s"flink[${mate.uri}] is unusable.")
              } else {
                Usability()
              }
          }

        }
      } catch {
        case e: ClassNotFoundException =>
          logger.error(e.getMessage)
          Usability(usable = false, cause = "plugin [flink] is not installed")
        case e: Exception =>
          logger.error(e.getMessage)
          Usability(usable = false, cause = s"check flink[${mate.uri}] has error:${e.getMessage}")
      } finally {
        if (flinkWatcher != null)
          flinkWatcher ! STOP
      }
    }).transform(Try(_), e => {
      e.printStackTrace()
      Try(Usability(usable = false, cause = s"use flink[${mate.uri}] fail: ${e.getCause}"))
    }
    ).get
  }
}
