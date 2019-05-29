package com.haima.sage.bigdata.etl.stream

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import akka.io.{IO, Tcp}
import akka.pattern.ask
import akka.remote.Ack
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model.Event
import com.haima.sage.bigdata.etl.utils.Logger

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by zhhuiyan on 16/5/10.
  */
class TCPStream(system: ActorSystem, host: Option[String], port: Int, encoding: Option[String] = None, listens: Map[String, String] = Map(), _timeout: Long) extends QueueStream[Event](None, _timeout) with Logger {

  implicit val timeout = Timeout(_timeout minutes)
  private val props = Props.create(classOf[TCPServer], this)
  val server: ActorRef = system.actorOf(props, "tcp_" + UUID.randomUUID())


  override def close(): Unit = {
    Await.result(server ? Tcp.Unbind, timeout.duration)
    super.close()
  }


  class TCPServer extends Actor {

    import Tcp._

    implicit val _system = context.system
    host match {
      case Some(_host: String) =>
        IO(Tcp) ! Bind(self, new InetSocketAddress(_host, port), pullMode = true)
      case _ =>
        IO(Tcp) ! Bind(self, new InetSocketAddress(port), pullMode = true)
    }

    def ready(socket: ActorRef): Receive = {

      case Unbind =>
        Await.result(socket ? Unbind, timeout.duration)
        context.stop(self)
        sender() ! "ok"
      case Unbound =>
        context.stop(self)

      case CommandFailed(_: Bind) =>
        context stop self

      case c@Connected(remote, _) =>
        logger.debug(s"find an new connected: $remote")
        val connection = sender()
        val handler = context.actorOf(Props.create(classOf[SimplisticHandler], this, connection, remote))
        connection ! Register(handler)
        connection ! ResumeAccepting(CACHE_SIZE.toInt)
    }

    def receive: PartialFunction[Any, Unit] = {
      case Bound(localAddress) =>
        sender ! ResumeAccepting(CACHE_SIZE.toInt)
        context.become(ready(sender()))
        logger.debug(s"tcp server bind:${localAddress.getHostName}:${localAddress.getPort}")
      // do some logging or setup ...
      case Unbound =>
        context.stop(self)
    }

    class SimplisticHandler(connection: ActorRef, remote: InetSocketAddress) extends Actor {

      import  context.dispatcher

      /*val task: Cancellable = context.system.scheduler.schedule(0 seconds, _timeout milliseconds)({
        if (queue.size() <= CACHE_SIZE.toInt) {
          connection ! ResumeReading
        }
      })*/

      override def preStart: Unit = connection ! ResumeReading

      import Tcp._

      def receive: PartialFunction[Any, Unit] = {
        case Ack => connection ! ResumeReading
        case CommandFailed(_: Connect) =>
          logger.debug(s"command failed close connected")
          //task.cancel()
          context stop self
        case Received(data) =>
          queue.add(Event(Some(Map[String, Any]("host_name" -> remote.getHostName,
            "host" -> remote.getAddress.getHostAddress,
            "port" -> remote.getPort
          )), data.decodeString(listens.getOrElse(remote.getHostString, encoding.getOrElse("utf-8"))).trim))
          if (queue.size() <= CACHE_SIZE.toInt) {
            connection ! ResumeReading
          }
        case PeerClosed =>
          logger.debug(s"$remote peer closed")
          // task.cancel()
          context stop self
        case _: ConnectionClosed =>
          logger.debug(s"connection closed")
          // task.cancel()
          context stop self
      }
    }

  }

}





