package com.haima.sage.bigdata.etl.writer

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Terminated}
import akka.io.Tcp._
import akka.io.Udp
import akka.util.ByteString
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.Status._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.metrics.MeterReport
import com.haima.sage.bigdata.etl.utils.Logger

import scala.concurrent.duration._


/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */
class NetDataWriter(conf: NetWriter, timer: MeterReport) extends DefaultWriter[NetWriter](conf, timer: MeterReport) with BatchProcess with Logger {
  private final val mapper = Formatter(conf.contentType)

  import context.dispatcher

  def getClient(protocol: Protocol): ActorRef = {
    protocol match {
      case TCP() =>
        conf.host match {
          case Some(h) =>
            context.actorOf(props[TCPClient](classOf[TCPClient], new InetSocketAddress(h, conf.port)))
          case _ =>
            context.actorOf(props[TCPClient](classOf[TCPClient], new InetSocketAddress(conf.port)))
        }

      case UDP() =>
        conf.host match {
          case Some(h) =>
            context.actorOf(props[UDPClient](classOf[UDPClient], new InetSocketAddress(h, conf.port)))
          case _ =>
            context.actorOf(props[UDPClient](classOf[UDPClient], new InetSocketAddress(conf.port)))
        }

      case Syslog(pro) =>
        getClient(pro)
    }
  }


  override def postStop(): Unit = {
    stopping = true
    if (task != 0) {
      logger warn s"force closed ${conf.protocol} connect lost data[$task]"
    }

    super.postStop()
  }

  private def _client(): ActorRef = getClient(conf.protocol.getOrElse(UDP()))

  private var client: ActorRef = _client()

  var task = 0

  override def write(t: RichMap): Unit = {
    if (!connect) {
      TimeUnit.SECONDS.sleep(1)
      if (!connect) {
        context.stop(client)
        client = _client()
      }
    }

    val data = mapper.bytes(t)
    client ! ByteString(data)
    if (getCached % cacheSize == 0) {
      this.flush()
    }


  }

  override def close(): Unit = {
    flush()
    if (task != 0) {
      logger.warn(s"close ${conf.protocol} connect lost data[${task}]")
    }
    context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
    client ! Close

    context.stop(self)
  }

  override def flush(): Unit = {

    if (task == 0) {
      report()
    } else {
      context.system.scheduler.scheduleOnce(1 seconds) {
        self ! Opt.FLUSH
      }

    }


  }


  override def redo(): Unit = {
  }


  override def receive: Receive = {
    case CommandFailed(w: Write) =>
      logger.debug(" write fail")
      if (connect) {
        client ! w.data
      } else {
        client = _client()

        client ! w.data
      }
    case data: ByteString =>
      logger.debug(" retry write data")
      if (connect) {
        client ! data
      } else {
        task += 1
        context.system.scheduler.scheduleOnce(1 seconds) {
          task -= 1
          self ! data
        }
      }
    case Opt.RESET if !stopping =>

      context.system.scheduler.scheduleOnce(1 seconds) {
        if (!stopping)
          client = _client()
      }

    case LOST_CONNECTED =>
      super.receive(LOST_CONNECTED)
      self ! Opt.RESET
    case any =>
      super.receive(any)
  }

  import java.net.InetSocketAddress

  import akka.actor.Props

  def props[F](clazz: Class[F], remote: InetSocketAddress): Props = Props(clazz, remote)


}

import akka.io.{IO, Tcp}
import akka.util.ByteString

class TCPClient(remote: InetSocketAddress) extends Actor with Logger {

  import Tcp._
  import context.system


  override def preStart(): Unit = {
    logger.debug(s"innit ----------------")
    IO(Tcp) ! Connect(remote)
  }


  def receive: PartialFunction[Any, Unit] = {


    case CommandFailed(_: Connect) =>
      logger.debug(s"CommandFailed ")

      context.parent ! LOST_CONNECTED
      context stop self
    case _: ConnectionClosed =>
      logger.debug(s"connection closed")
    case Connected(_, _) =>
      context.watch(sender())
      context.parent ! CONNECTED
      logger.debug(s"connected ")
      val connection = sender()
      connection ! Register(self)
      context become {
        case Terminated(_) =>
          logger.debug(s"connection closed")
          context.parent ! LOST_CONNECTED
          context stop self
        case data: ByteString =>
          logger.debug(s" tcp writer data ---" + data.decodeString("utf-8"))
          connection ! Write(data.concat(ByteString("\n")))
        case CommandFailed(w: Write) =>
          logger.debug(s" tcp writer fail ---")
          context.parent ! LOST_CONNECTED
          context.parent ! CommandFailed(w: Write)
          context stop self
        case Received(data) =>
          logger.warn(s"ignore remote msg:${data.decodeString("utf-8")}")
        case Close =>
          connection forward Close
        case msg: ConnectionClosed =>
          context.parent ! LOST_CONNECTED
          logger.debug(s"connection closed")
          context stop self
      }
    case data: ByteString =>
      logger.debug(s"remote not ready ")
      context.parent ! data
  }
}

class UDPClient(remote: InetSocketAddress) extends Actor with Logger {

  import context.system

  IO(Udp) ! Udp.SimpleSender

  def receive: PartialFunction[Any, Unit] = {
    case Udp.SimpleSenderReady =>
      context.parent ! CONNECTED
      context.become(ready(sender()))
    case Udp.Unbound =>
      context.stop(self)
  }

  def ready(send: ActorRef): Receive = {
    case msg: ByteString =>


      send ! Udp.Send(msg, remote)
    case Udp.Unbound =>
      context.stop(self)
  }
}