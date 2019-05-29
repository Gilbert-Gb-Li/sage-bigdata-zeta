package com.haima.sage.bigdata.etl.server.worker

import akka.actor.{Actor, ActorIdentity, ActorPath, ActorRef, DeadLetter, Identify, Terminated}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.remote._
import com.haima.sage.bigdata.etl.common.model.Opt
import org.slf4j.LoggerFactory

import scala.concurrent.duration._


trait WithRemote extends Actor {
  private lazy val logger = LoggerFactory.getLogger(classOf[WithRemote])

  import com.haima.sage.bigdata.etl.common.Constants.connectWithCluster

  /**
    * 集群服务地址
    **/
  def paths: Set[ActorPath]

  /**
    * 服务名称
    **/
  def serverName: String

  lazy val cluster = context.actorOf(ClusterClient.props(ClusterClientSettings(context.system).withInitialContacts(paths)))
  lazy val single = context.actorSelection(paths.head)

  /**
    * 发送数据到远端
    */
  lazy val send: Any => Unit = if (connectWithCluster) {
    msg: Any => {
      cluster ! ClusterClient.Send(serverName, msg, localAffinity = true)
    }
  } else {
    msg: Any => {
      single ! msg
    }

  }
  /**
    * 转发数据到远端
    ***/
  lazy val forward: Any => Unit = if (connectWithCluster) {
    msg: Any => {
      cluster forward ClusterClient.Send(serverName, msg, localAffinity = true)
    }
  } else {
    msg: Any => {
      single forward msg
    }

  }

  override def preStart(): Unit = {
    identify()
  }

  def identify(): Unit = {
    send(Identify("server"))
  }

  /**
    * 连接上远端以后的操作
    *
    * @param remote
    * @return
    */
  def withRemote(remote: ActorRef): Receive

  /**
    * 监听远端
    *
    * @param remote
    * @return
    */
  private def remoteWatch(remote: ActorRef): Receive = {

    case Terminated(actor) =>
      logger.debug(s"unwatch $remote")
      context.unwatch(actor)
      context.unbecome()
      identify()
  }

  /**
    * 连接上后的动作
    *
    * @param remote
    */
  def onConnect(remote: ActorRef): Unit

  import context.dispatcher

  override def receive: Receive = {
    case Opt.RESTART =>
      logger.info(s" ${this.getClass.getName},master[${sender()}] server restart")
      onConnect(sender())
    //logger.debug("DisassociatedEvent")
    case QuarantinedEvent(_, _) =>
      logger.debug("QuarantinedEvent")
    //identify()
    case RemotingErrorEvent(cause) =>
      logger.debug("RemotingErrorEvent")
    case RemotingShutdownEvent =>
      logger.debug("RemotingShutdownEvent")


    case ActorIdentity(_, Some(actor)) =>
      // context.watch(actor)
      onConnect(actor)
      if (connectWithCluster) {
        context.actorSelection("/user/relay-server") ! Opt.RESTART
      }

      logger.info(s"connected with remote $paths")
      context.become(withRemote(actor).orElse(remoteWatch(actor)).orElse(receive))
    case ActorIdentity(_, None) =>
      //表示本地Server端启动,Restful_Server端未启动
      logger.warn(s"remote[$paths] not available")
      //每隔1秒:请求Restful Server是否开启
      context.system.scheduler.scheduleOnce(1 seconds) {
        identify()
      }
    case DisassociatedEvent(_, remoteAddress, _) =>
      logger.warn(s"${this.getClass.getName},disassociated with remote[$paths] ")

    case AssociatedEvent(_, remoteAddress, _) =>
       logger.info(s"${this.getClass.getName},associated with $remoteAddress")
      if(paths.exists(_.address==remoteAddress)){
        logger.info(s"${this.getClass.getName},associated with $remoteAddress,identify with master")
        identify()
      }



    case AssociationErrorEvent(cause, localAddress, remoteAddress, _, _) =>
      logger.error(s" association error with $remoteAddress")
    case t: DeadLetter =>
      logger.warn(s"$t")
    case msg =>
      logger.warn(s"ignore msg:send[${sender()}] class:${msg.getClass},data:$msg")

  }
}
