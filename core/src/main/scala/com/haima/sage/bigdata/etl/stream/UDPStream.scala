package com.haima.sage.bigdata.etl.stream

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.{IO, Udp}
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model.Event
import com.haima.sage.bigdata.etl.utils.Logger

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by zhhuiyan on 16/5/10.
  */
class UDPStream(system: ActorSystem, host: Option[String], port: Int, encoding: Option[String] = None, listens: Map[String, String] = Map(), _timeout: Long) extends QueueStream[Event](None, _timeout) with Logger {

  implicit val timeout = Timeout(_timeout minutes)
  private val props = Props.create(classOf[Listener], this)
  val name = "ucp_" + UUID.randomUUID()
  val server: ActorRef = system.actorOf(props, name)


  override def close(): Unit = {
    Await.result(server ? Udp.Unbind, timeout.duration)


    super.close()
  }

  class Listener extends Actor {


    host match {
      case Some(_host: String) =>
        IO(Udp)(context.system) ! Udp.Bind(self, new InetSocketAddress(_host, port))
      case _ =>
        IO(Udp)(context.system) ! Udp.Bind(self, new InetSocketAddress(port))
    }


    def receive: PartialFunction[Any, Unit] = {
      case Udp.Bound(localAddress) =>
        logger.info(s"udp server bind:${localAddress.getHostName}:${localAddress.getPort}")
        context.become(ready(sender()))
      case Udp.Unbound =>
        context.stop(self)
    }

    var cached = 0

    def ready(socket: ActorRef): Receive = {
      case Udp.Received(data, remote) =>
        if (queue.size() >= CACHE_SIZE) {
          if (cached % 100 == 0) {
            logger.warn(s"event times[${cached + 1}], queue[$CACHE_SIZE] is full,drop data[${data.decodeString("gbk")}]")
           // cached = 0

          }

          cached += 1
        } else {
          queue.add(Event(Some(Map[String, Any]("host_name" -> remote.getHostName,
            "host" -> remote.getAddress.getHostAddress,
            "port" -> remote.getPort
          )), data.decodeString(listens.getOrElse(remote.getHostString, encoding.getOrElse("utf-8")))))
        }


      // socket ! Udp.Send(data, remote) // example server echoes back

      case Udp.Unbind =>
        Await.result(socket ? Udp.Unbind, timeout.duration)
        context.stop(self)
        sender() ! "ok"
    }
  }

}
