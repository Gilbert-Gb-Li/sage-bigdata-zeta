package com.haima.sage.bigdata.analyzer.modeling.client

import akka.actor.{Actor, ActorRef}
import com.haima.sage.bigdata.etl.common.model.Opt
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.client.program.{ClusterClient, ProgramInvocationException, StandaloneClusterClient}
import org.apache.flink.configuration.{Configuration, JobManagerOptions}
import org.apache.flink.runtime.client.JobCancellationException
import org.apache.flink.runtime.jobgraph.JobGraph
import org.slf4j.{Logger, LoggerFactory}

class JobRunner(address: String,
                port: String,
                job: JobGraph,
                env: ExecutionEnvironment,
                report: ActorRef) extends Actor {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[JobRunner])
  private val flinkConfig = {
    val conf = new Configuration
    conf.setString(JobManagerOptions.ADDRESS, address)
    conf.setInteger(JobManagerOptions.PORT, port.toInt)
    conf
  }
  private lazy val client: ClusterClient[_] = try {
    val _client = new StandaloneClusterClient(flinkConfig)
    _client.setPrintStatusDuringExecution(false)
    _client
  } catch {
    case e: Exception =>
      e.printStackTrace()
      throw new ProgramInvocationException("Cannot establish connection to JobManager: " + e.getMessage, e)
  }

  def getRootCause(ex: Exception): Throwable = {
    if (ExceptionUtils.getRootCause(ex) != null) ExceptionUtils.getRootCause(ex) else ex
  }

  override def receive: Receive = {
    case Opt.START =>
      try {
        client.setDetached(true)
        client.submitJob(job, env.getJavaEnv.getClass.getClassLoader)
      } catch {
        case ex: Exception =>
          val error = getRootCause(ex)
          error match {
            case cancel: JobCancellationException =>
              logger.info(cancel.getMessage)
            case _ =>
              logger.error(s"Submit flink job[${job.getJobID}] error:", ex)
              report ! (Opt.STOP, error.getMessage)
          }
      } finally {
        client.shutdown()
      }

  }
}