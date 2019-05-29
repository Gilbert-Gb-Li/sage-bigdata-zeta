package com.haima.sage.bigdata.etl.server

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorPath, ActorSystem, DeadLetter, Props}
import akka.remote.{AssociatedEvent, AssociationErrorEvent, DisassociatedEvent}
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.model.Opt
import com.haima.sage.bigdata.etl.metrics.{AkkaReporter, MetricRegistrySingle}
import com.haima.sage.bigdata.etl.server.worker._
import com.haima.sage.bigdata.etl.utils.Logger

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Hello world!
  *
  */
object Worker extends App {

  class Worker

  Logger.init("logback-worker.xml")

  protected lazy val logger = Logger(classOf[Worker]).logger

  implicit val timeout = Timeout(5 minutes)

  import scala.collection.JavaConversions._

  Constants.init("worker.conf")

  final val system = ActorSystem("worker", CONF.resolve())


  private val paths = if (connectWithCluster) {
    CONF.getStringList(REMOTE).map(item =>
      ActorPath.fromString(s"$item/system/receptionist")
    ).toSet
  } else {
    CONF.getStringList(REMOTE).map(item =>
      ActorPath.fromString(s"$item/user/relay-server")
    ).toSet
  }
  //final val master = s"akka.tcp://${CONF.getString(REMOTE_TYPE)}@${CONF.getString(REMOTE_HOST)}:${CONF.getInt(REMOTE_PORT)}/user/server"


  system.actorOf(Props[Publisher].withDispatcher("akka.dispatcher.executor"), "publisher")

  system.actorOf(Props[PositionServer], "position")
  system.actorOf(Props[CacheServer], "cache")
  system.actorOf(Props(classOf[StatusWatcher], paths).withDispatcher("akka.dispatcher.server"), "watcher")

  val metricWatcherActor = system.actorOf(Props(classOf[MetricWatcher]).withDispatcher("akka.dispatcher.metric"), name = "metric")
  AkkaReporter.forRegistry(MetricRegistrySingle(system), metricWatcherActor).build.start(CONF.getDuration(Constants.METRIC_INTERVAL_SECOND).getSeconds, TimeUnit.SECONDS)

  private val server = system.actorOf(Props(classOf[Server], paths).withDispatcher("akka.dispatcher.server"), name = "server")

  if (connectWithCluster) {
    /*启动传输服务*/
    system.actorOf(Props(classOf[RelayServer], paths).withDispatcher("akka.dispatcher.server"), name = "relay-server")
  }

  val listener = system.actorOf(Props(new Actor {
    def receive: PartialFunction[Any, Unit] = {
      case DisassociatedEvent(local, remote, inbound) =>
        context.actorSelection("/user/server") ! DisassociatedEvent(local, remote, inbound)
        context.actorSelection("/user/metric") ! DisassociatedEvent(local, remote, inbound)
      case AssociatedEvent(local, remote, _) =>
        logger.debug(s"AssociatedEvent : remote = $remote local = $local")
      /*  server ! AssociatedEvent(local, remote, inbound)
        metricServer ! AssociatedEvent(local, remote, inbound)*/
      case AssociationErrorEvent(cause, local, remote, inbound, _) =>
        logger.warn(s"The remote system has quarantined this system. No further associations to the remote system are possible until this system is restarted.")
      /*context.actorSelection("/user/server") ! DisassociatedEvent(local, remote, inbound)
      context.actorSelection("/user/metric") ! DisassociatedEvent(local, remote, inbound)*/
      case letter: DeadLetter =>
        letter.message match {
          case (_: String, log: Map[String@unchecked, Any@unchecked]) =>
            letter.sender ! log
          case _ =>
            logger.warn(s" from[${letter.sender.path}] to[${letter.recipient.path}] deadLetter[${letter.message}} ]")
        }
    }
  }))

  // listen to Remote Actor Lifecycle
  system.eventStream.subscribe(server, classOf[DeadLetter])
  system.eventStream.subscribe(server, classOf[akka.remote.RemotingLifecycleEvent])
  //system.eventStream.subscribe(server, classOf[DeadLetter])
  //system.eventStream.subscribe(listener, classOf[DeadLetter])
  sys.addShutdownHook {
    import akka.pattern._
    implicit val timeout = Timeout(5 minutes)
    logger.info("shutdown ,please waiting ....")
    val future = system.actorSelection("/user/server") ? Opt.STOP

    if (Await.result(future, timeout.duration).asInstanceOf[Boolean]) {
      logger.info("shutdown finished ")
    } else {
      logger.error("shutdown finished ,but something could not work well ,please check it")
    }
    system.terminate()
  }
}


