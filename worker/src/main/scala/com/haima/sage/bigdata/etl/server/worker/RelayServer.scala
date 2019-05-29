package com.haima.sage.bigdata.etl.server.worker

import akka.actor.{ActorPath, ActorRef, Identify}
import com.haima.sage.bigdata.etl.common.model.{Collector, Opt}
import org.slf4j.LoggerFactory

class RelayServer(val paths: Set[ActorPath]) extends WithRemote {
  private lazy val logger = LoggerFactory.getLogger(classOf[RelayServer])

  override val serverName: String = "/user/server"

  private def getPath(worker: Collector, name: String = "server") = {


    s"akka.tcp://${worker.system}@${worker.host}:${worker.port}/user/$name"
  }

  def listener(workers: Set[Collector]): Receive = {
    case Opt.RESTART =>
      workers.foreach(worker => {
        context.actorSelection(getPath(worker)) ! Opt.RESTART
      })

    case (worker: Collector, Opt.SEND, msg) =>
      context.unbecome()
      context.become(listener(workers + worker).orElse(receive))
      context.actorSelection(getPath(worker)) ! msg

    case (ref: ActorRef, Opt.SEND, msg) =>
      ref ! msg
    case (ref: ActorRef, Opt.FORWARD, msg) =>


      ref.forward(msg)
    case ("status-server", Identify("status-server")) =>
      //  forwardToRemote(Identify("status-server"))
      forward(Identify("status-server"))
    case msg =>
      forward(msg)

  }


  override def withRemote(remote: ActorRef): Receive = listener(Set())

  override def onConnect(remote: ActorRef): Unit = {
    logger.info("relay server connect running")
    self ! Opt.RESTART
  }
}
