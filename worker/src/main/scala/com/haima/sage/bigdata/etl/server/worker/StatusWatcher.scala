package com.haima.sage.bigdata.etl.server.worker

import akka.actor.{ActorIdentity, ActorLogging, ActorPath, ActorRef, Identify, Terminated}
import akka.cluster.client.ClusterClient
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Logger

class StatusWatcher(override val paths: Set[ActorPath]) extends WithRemote with ActorLogging with Logger {
  /**
    * 服务名称
    **/
  override val serverName: String = "/user/status-server"




  override def onConnect(remote: ActorRef): Unit = {
    logger.info("status watcher connect running")
  }

  override def withRemote(remote: ActorRef): Receive = {
   case (config: Config, ProcessModel.MONITOR, Status.UNKNOWN, msg: String) =>
      logger.info(s"Worker server exception:$msg")
      send(RunStatus(config.id, None, ProcessModel.MONITOR, Status.RUNNING, Option(msg)))
    case (Status.LOST_CONNECTED, masters) =>
      logger.warn(s"status server is waiting with " + masters)
      context.unbecome()
    case (config: Config, ProcessModel.MONITOR, status: Status.Status) =>
      send(RunStatus(config.id, None, ProcessModel.MONITOR, status))
    case (config: Config, ProcessModel.MONITOR, status: Status.Status, msg: String) =>
      send(RunStatus(config.id, None, ProcessModel.MONITOR, status, Option(msg)))
    //stream
    case (config: Config, ProcessModel.STREAM, path: String, status: Status.Status) =>
      send(RunStatus(config.id, Option(path), ProcessModel.STREAM, status))
    case (config: Config, ProcessModel.STREAM, path: String, status: Status.Status, msg: String) =>
      send(RunStatus(config.id, Option(path), ProcessModel.STREAM, status, Option(msg)))
    //writer
    case (config: Config, ProcessModel.WRITER, path: String, status: Status.Status) =>
      send(RunStatus(config.id, Option(path), ProcessModel.WRITER, status))
    case (config: Config, ProcessModel.WRITER, path: String, status: Status.Status, msg: String) =>
      send(RunStatus(config.id, Option(path), ProcessModel.WRITER, status, Option(msg)))
    //lexer
    case (config: Config, ProcessModel.LEXER, path: String, status: Status.Status) =>
      send(RunStatus(config.id, Option(path), ProcessModel.LEXER, status))
    case (config: Config, ProcessModel.LEXER, path: String, status: Status.Status, msg: String) =>
      send(RunStatus(config.id, Option(path), ProcessModel.LEXER, status, Option(msg)))
    //modeling
    case (modeling: ModelingConfig, ProcessModel.MONITOR, status: Status.Status) =>
      send(RunStatus(modeling.id, None, ProcessModel.MONITOR, status))
    case (modeling: ModelingConfig, ProcessModel.MONITOR, status: Status.Status, msg: String) =>
      send(RunStatus(modeling.id, None, ProcessModel.MONITOR, status, Option(msg)))
    //modeling writer
    case (modelingConfig: ModelingConfig, ProcessModel.WRITER, path: String, status: Status.Status) =>
      send(RunStatus(modelingConfig.id, Option(path), ProcessModel.WRITER, status))
    case (modelingConfig: ModelingConfig, ProcessModel.WRITER, path: String, status: Status.Status, msg: String) =>
      send(RunStatus(modelingConfig.id, Option(path), ProcessModel.WRITER, status, Option(msg)))

    //knowledge status
    case (knowledgeId: String, ProcessModel.KNOWLEDGE, status: Status.Status) =>
      send((knowledgeId, ProcessModel.KNOWLEDGE, status))

    case (Opt.PUT, status: RunStatus) =>
      forward(status)

    case (configId: String, ProcessModel.MONITOR) =>
      forward((configId, ProcessModel.MONITOR))

    case configId: String =>
      forward(configId)

    case (Opt.GET, configId: String, ProcessModel.MONITOR) =>
      forward((configId, ProcessModel.MONITOR))

    case (Opt.GET, configId: String) =>
      forward(configId)

    case Opt.GET =>
      forward(Opt.GET)
  }

}
