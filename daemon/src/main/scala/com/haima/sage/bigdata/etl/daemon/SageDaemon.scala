package com.haima.sage.bigdata.etl.daemon

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.haima.sage.bigdata.etl.utils.Logger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.daemon.{Daemon, DaemonContext, DaemonUserSignal}

import scala.concurrent.duration._


/**
  * Hello world!
  *
  */
class SageDaemon extends Daemon with DaemonUserSignal {

  class SageDaemon

  Logger.init("logback-daemon.xml")
  var conf: Config = _
  final var system: ActorSystem = _
  /*var masters: Set[ActorPath] = _*/


  override def init(daemonContext: DaemonContext): Unit = {
    conf = ConfigFactory.load("daemon.conf")
    /*masters = conf.getConfigList("remote").map(obj =>
      ActorPath.fromString(s"akka.tcp://${obj.getString("cluster")}@${obj.getString("host")}:${obj.getInt("port")}/system/receptionist")
    ).toSet*/
  }

  override def start(): Unit = {
    implicit val timeout = Timeout(5 minutes)
    system = ActorSystem("daemon", conf)
    system.actorOf(Props(classOf[Server]), name = "server")
  }

  override def destroy(): Unit = {
    System.err.println("SageDaemon: destroyed")
  }

  override def stop(): Unit = {
    System.err.println("SageDaemon: stop server")
    system.terminate()
  }

  override def signal(): Unit = {
    System.err.println("SageDaemon: ignore signal")
  }
}

object SageDaemon extends App {
  val daemon = new SageDaemon()
  daemon.init(null)
  daemon.start()
}


