package com.haima.sage.bigdata.etl.reader

import java.net.InetSocketAddress

import com.haima.sage.bigdata.etl.utils.Logger
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, ByteBufUtil, Unpooled}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.CharsetUtil

/**
  * Created by zhhuiyan on 16/4/26.
  */

object EchoClient {


  def main(args: Array[String]) {

    val client = new EchoClient("127.0.0.1", 12025)
    client.start()
    client.send("")

  }
}

class EchoClient(host: String, port: Int) extends Logger {
  var channel: Channel = _

  def send(msg: String): Unit = {
    channel.writeAndFlush(msg)
  }

  def start(): Unit = {
    val group: EventLoopGroup = new NioEventLoopGroup()
    try {
      val b = new Bootstrap()
      b.group(group)
      b.channel(classOf[NioSocketChannel])
      b.remoteAddress(new InetSocketAddress(host, port))
      b.handler(new ChannelInitializer[SocketChannel]() {

        override def initChannel(ch: SocketChannel) {
          ch.pipeline().addLast(new EchoClientHandler())
        }
      })

      val f: ChannelFuture = b.connect().sync()


      f.addListener(new ChannelFutureListener() {

        def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            logger.debug("client connected")
          } else {
            logger.debug("server attemp failed")
            future.cause().printStackTrace()
          }

        }
      })
      channel = f.channel()
      f.channel().closeFuture().sync();
    } finally {
      group.shutdownGracefully().sync()
    }
  }


}

@Sharable
class EchoClientHandler extends SimpleChannelInboundHandler[ByteBuf]  with Logger {
  /**
    * 此方法会在连接到服务器后被调用
    **/
  override def channelActive(ctx: ChannelHandlerContext) {
    ctx.write(Unpooled.copiedBuffer("Netty rocks!", CharsetUtil.UTF_8));
  }

  /**
    * 此方法会在接收到服务器数据后调用
    **/
  def channelRead0(ctx: ChannelHandlerContext, in: ByteBuf) {
    logger.debug("Client received: " + ByteBufUtil.hexDump(in.readBytes(in.readableBytes())));
  }

  /**
    * 捕捉到异常
    **/
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close()
  }

}

