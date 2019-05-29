package com.haima.sage.bigdata.etl.server

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

/**
  * Created by zhhuiyan on 2017/2/17.
  */
object TCPServer$ extends App {
  final val system = ActorSystem("worker", ConfigFactory.load("src/test/resources/serverTest.conf"))
  system.actorOf(Props[TCPServer], "tcpserver")
}


class TCPServer extends Actor {

  import Tcp._
  import context.system

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 5000))

  def receive = {
    case b@Bound(localAddress) =>
      println(s"tcp server bind:${localAddress.getHostName}:${localAddress.getPort}")
    // do some logging or setup ...

    case CommandFailed(_: Bind) =>
      context stop self

    case c@Connected(remote, local) =>
      val handler = context.actorOf(Props[SimplisticHandler])
      val connection = sender()
      connection ! Register(handler)
    case Unbound =>
      context stop self
  }

}


class SimplisticHandler extends Actor {

  import Tcp._

  def receive = {
    case Received(data) =>
      println(new String(data.toArray))
      sender() ! Write(data)
    case PeerClosed => context stop self
  }
}

class SimpleEchoHandler(connection: ActorRef, remote: InetSocketAddress)
  extends Actor with ActorLogging {

  import Tcp._

  // sign death pact: this actor terminates when connection breaks
  context watch connection

  case object Ack extends Event

  var storage: List[ByteString] = List()
  var stored: Int = 0
  var transferred: Int = 0
  val maxStored: Int = 1000
  val highWatermark: Int = 500
  val lowWatermark: Int = 10
  var closing: Boolean = false
  var suspended: Boolean = false

  def receive = {
    case Received(data) =>
      buffer(data)
      connection ! Write(data, Ack)

      context.become({
        case Received(data) => buffer(data)
        case Ack => acknowledge()
        case PeerClosed => closing = true
      }, discardOld = false)

    case PeerClosed => context stop self
  }

  private def buffer(data: ByteString): Unit = {
    storage :+= data
    stored += data.size

    if (stored > maxStored) {
      log.warning(s"drop connection to [$remote] (buffer overrun)")
      context stop self

    } else if (stored > highWatermark) {
      log.debug(s"suspending reading")
      connection ! SuspendReading
      suspended = true
    }
  }

  private def acknowledge(): Unit = {
    require(storage.nonEmpty, "storage was empty")

    val size = storage(0).size
    stored -= size
    transferred += size

    storage = storage drop 1

    if (suspended && stored < lowWatermark) {
      log.debug("resuming reading")
      connection ! ResumeReading
      suspended = false
    }

    if (storage.isEmpty) {
      if (closing) context stop self
      else context.unbecome()
    } else connection ! Write(storage(0), Ack)
  }

  // storage omitted ...
}

/*object EchoHandler {

  final case class Ack(offset: Int) extends Tcp.Event

  var storage: List[ByteString] = List()
  var stored: Int = 0
  var transferred: Int = 0
  val maxStored: Int = 1000
  val highWatermark: Int = 500
  val lowWatermark: Int = 10
  var closing: Boolean = false
  var suspended: Boolean = false

  def props(connection: ActorRef, remote: InetSocketAddress): Props =
    Props(classOf[EchoHandler], connection, remote)
}*/

/*class EchoHandler(connection: ActorRef, remote: InetSocketAddress)
  extends Actor with ActorLogging {

  import EchoHandler._
  import Tcp._

  // sign death pact: this actor terminates when connection breaks
  context watch connection

  // start out in optimistic write-through mode
  def receive = writing

  def writing: Receive = {
    case Received(data) =>
      connection ! Write(data, Ack(currentOffset))
      buffer(data)

    case Ack(ack) =>
      acknowledge(ack)

    case CommandFailed(Write(_, Ack(ack))) =>
      connection ! ResumeWriting
      context become buffering(ack)

    case PeerClosed =>
      if (storage.isEmpty) context stop self
      else context become closing
  }

  def buffering(nack: Int): Receive = {
    var toAck = 10
    var peerClosed = false

    {
      case Received(data) => buffer(data)
      case WritingResumed => writeFirst()
      case PeerClosed => peerClosed = true
      case Ack(ack) if ack < nack => acknowledge(ack)
      case Ack(ack) =>
        acknowledge(ack)
        if (storage.nonEmpty) {
          if (toAck > 0) {
            // stay in ACK-based mode for a while
            writeFirst()
            toAck -= 1
          } else {
            // then return to NACK-based again
            writeAll()
            context become (if (peerClosed) closing else writing)
          }
        } else if (peerClosed) context stop self
        else context become writing
    }
  }

  // buffering ...
  def closing: Receive = {
    case CommandFailed(_: Write) =>
      connection ! ResumeWriting
      context.become({

        case WritingResumed =>
          writeAll()
          context.unbecome()

        case ack: Int => acknowledge(ack)

      }, discardOld = false)

    case Ack(ack) =>
      acknowledge(ack)
      if (storage.isEmpty) context stop self
  }

  // closing ...
  private def buffer(data: ByteString): Unit = {
    storage :+= data
    stored += data.size

    if (stored > maxStored) {
      log.warning(s"drop connection to [$remote] (buffer overrun)")
      context stop self

    } else if (stored > highWatermark) {
      log.debug(s"suspending reading at $currentOffset")
      connection ! SuspendReading
      suspended = true
    }
  }

  private def acknowledge(ack: Int): Unit = {
    require(ack == storageOffset, s"received ack $ack at $storageOffset")
    require(storage.nonEmpty, s"storage was empty at ack $ack")

    val size = storage(0).size
    stored -= size
    transferred += size

    storageOffset += 1
    storage = storage drop 1

    if (suspended && stored < lowWatermark) {
      log.debug("resuming reading")
      connection ! ResumeReading
      suspended = false
    }
  }

  override def postStop(): Unit = {
    log.info(s"transferred $transferred bytes from/to [$remote]")
  }

  // storage omitted ...
}*/

// storage omitted ...