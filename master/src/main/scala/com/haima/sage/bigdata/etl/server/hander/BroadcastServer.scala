package com.haima.sage.bigdata.etl.server.hander

import akka.actor.{Actor, RootActorPath}
import akka.cluster.Member
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.Opt
import com.haima.sage.bigdata.etl.utils.{Logger, Mapper}

import scala.concurrent.{Await, Future}

/**
  * Created by zhhuiyan on 16/7/18.
  */
trait BroadcastServer extends Actor with Logger with Mapper {
  implicit lazy val timeout = Timeout(Constants.getApiServerConf(Constants.MASTER_SERVICE_TIMEOUT).toLong, java.util.concurrent.TimeUnit.SECONDS)

  override def receive: Receive = {
    /* case Result(_, message) =>
       logger.debug(s" ignore  result:$message")*/
    case (Opt.SYNC, msg) =>
      logger.debug(s" ignore sync result:$msg")
    case msg =>
      broadcast(msg: Any)
  }

  def broadcast(msg: Any, response: String = "received"): Unit = {
    context.actorSelection("/user/listener") ! msg
    self.forward((Opt.SYNC, msg))

  }

  def remotePath(member: Member): String = {

    RootActorPath(member.address).toString + context.self.path.toStringWithoutAddress
  }

  def notBroadcast: Boolean = {

    val rt = Await.result((context.actorSelection("/user/listener") ? "isLocal").asInstanceOf[Future[Boolean]], timeout.duration)
    logger.info(s"brothers:" + rt)
    rt
    /*logger.info(s"brothers:" + Master.brothers)
    Master.brothers.isEmpty || !Master.brothers.exists(member => {
      member.uniqueAddress.address == sender().path.address
    })*/
  }


}
