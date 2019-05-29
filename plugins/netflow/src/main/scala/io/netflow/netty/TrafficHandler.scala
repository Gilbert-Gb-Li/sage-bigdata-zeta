package io.netflow.netty

import java.net.InetSocketAddress

import com.haima.sage.bigdata.etl.utils.Logger
import io.netty.buffer._
import io.netty.channel._
import io.netty.channel.socket.DatagramPacket


abstract class TrafficHandler extends SimpleChannelInboundHandler[DatagramPacket] with Logger {

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
    e.printStackTrace()
  }

  def handler(sender: InetSocketAddress, buf: ByteBuf): Unit

  override def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket) {
    val sender = msg.sender

    // The first two bytes contain the NetFlow version and first four bytes the sFlow version
    if (msg.content().readableBytes() < 4) {
      logger.warn("Unsupported UDP Packet received from " + sender.getAddress.getHostAddress + ":" + sender.getPort)
      return
    }

    // Retain the payload
    msg.content().retain()
    handler(sender, msg.content())
  }
}