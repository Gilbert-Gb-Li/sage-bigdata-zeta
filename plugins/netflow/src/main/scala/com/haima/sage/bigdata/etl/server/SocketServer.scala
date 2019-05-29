package com.haima.sage.bigdata.etl.server

import java.net.InetSocketAddress

import com.haima.sage.bigdata.etl.utils.Logger
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioDatagramChannel

import scala.util.{Failure, Success, Try}


/**
  * Created by zhhuiyan on 16/4/15.
  */
object SocketServer {

  def apply(host: String = "0.0.0.0", port: Int = 2025, receiveBuf: Int, handler: Option[ChannelHandler]): SocketServer = new SocketServer(host, port, receiveBuf, handler)

}

class SocketServer(host: String = "0.0.0.0", port: Int = 2025, receiveBuf: Int, handler: Option[ChannelHandler]) extends Logger {

  lazy val eventLoop: EventLoopGroup = new NioEventLoopGroup
  lazy val address: Option[InetSocketAddress] = host match {
    case ipv4: String if ipv4.matches("""\d+\.\d+\.\d+\.\d+""") =>
      Try {
        new java.net.InetSocketAddress(ipv4, port)
      }.toOption
    case ipv6: String if ipv6.matches("""[0-9a-fA-F:]+\]""") =>
      Try(new java.net.InetSocketAddress(java.net.InetAddress.getByName(ipv6), port)).toOption
    case _ => None
  }

  var channel: ChannelFuture = _

  def start(): Unit = {
    Try {

      handler match {
        case Some(h) =>
          val boot = new Bootstrap

          boot.group(eventLoop)
            .localAddress(address.get)
            .channel(classOf[NioDatagramChannel])

            .handler(h)
            .option[java.lang.Integer](ChannelOption.SO_RCVBUF, receiveBuf)
          channel = boot.bind().sync


        case None =>
          throw new NotImplementedError()
      }


    } match {
      case Success(v) =>
        logger.info(s"Listening  on ${address.get.getAddress.getHostAddress}:${address.get.getPort} message:${v}")
      case Failure(f) =>
        logger.error(s"Unable to bind  ${address.get.getAddress.getHostAddress}:${address.get.getPort} combination. Check your configuration.")
        if (logger.isDebugEnabled)
          f.printStackTrace()

    }
  }

  def stop(): Unit = {
    if (this.channel != null) {
      this.channel.addListener(ChannelFutureListener.CLOSE)
      channel.cancel(true)
      this.channel=null
    }
  }

}
