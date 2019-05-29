package com.haima.sage.bigdata.etl.plugin.flink

import akka.actor.{Actor, ActorRef}
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Logger

import scala.concurrent.duration._

trait FlinkStatusProcessor extends Actor with Logger {


  def flinkWatcher: ActorRef


  def apply(jobId: String): PartialFunction[Any, Unit] = {

    flinkWatcher ! (jobId, FlinkAPIs.JOB_STATUS)


    {


      case Status.UNKNOWN =>
        retry(jobId, Status.UNKNOWN)

      case Status.UNAVAILABLE =>
        retry(jobId, Status.UNAVAILABLE)
      case Status.RUNNING =>
        retry(jobId, Status.RUNNING)
      case status: Status.Status =>
        stop(jobId, status)

      // logger.info(s"Flink job [$jobId] is running.")
      // watcher ! (ProcessModel.ANALYZER, Status.RUNNING)
    }
  }


  private def stop(jobId: String, status: Status.Status): Unit = {
    self ! (Opt.STOP, s"Flink job [$jobId] execute ${status}.")
    flinkWatcher ! Opt.STOP
  }


  private def retry(jobId: String, status: Status.Status): Unit = {
    logger.debug(s"Flink job [$jobId] is $status, watcher running per 10 seconds.")
    context.system.scheduler.scheduleOnce(10 seconds)(
      flinkWatcher ! (jobId, FlinkAPIs.JOB_STATUS)
    )(context.dispatcher)

  }


}
