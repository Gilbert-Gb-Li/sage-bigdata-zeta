package com.haima.sage.bigdata.etl.stream

import java.io.IOException
import java.net.InetSocketAddress

import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{RichMap, Stream}
import com.haima.sage.bigdata.etl.server.{SocketServer, TemplateProc}
import com.haima.sage.bigdata.etl.utils.Logger
import io.netflow.flows.cflow
import io.netflow.flows.cflow.Template
import io.netflow.lib.FlowPacket
import io.netflow.netty.TrafficHandler
import io.netty.buffer.ByteBuf

import scala.util.Try

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object NetFlowStream {
  def apply(host: String = "0.0.0.0", port: Int = 2025, _timeout: Long): Stream[RichMap] = new NetFlowStream(host, port, _timeout)


}

class NetFlowStream(host: String = "0.0.0.0", port: Int = 2025, _timeout: Long) extends QueueStream[RichMap](None, _timeout) with Logger {
  implicit val timeout = Timeout(5, java.util.concurrent.TimeUnit.MINUTES)


  val client = new NetFlowClient(this)
  client.start()


  var count = 0l


  @throws(classOf[IOException])
  override def close() {
    super.close()
    finished()
    client.stop()

  }


  def getCache = CACHE_SIZE.toInt + 1

  def getHost = host

  def getPort = port


}

class NetFlowClient(stream: NetFlowStream) extends TrafficHandler with TemplateProc[cflow.Template] with Logger {

  private val socket = SocketServer(stream.getHost, stream.getPort, stream.getCache * 1024 * 1024, Some(this))

  def start(): Unit = {
    socket.start()
  }

  def stop(): Unit = {
    socket.stop()
  }

  private var cache: Map[Int, cflow.Template] = Map()

  def templates: Map[Int, Template] = cache

  def setTemplate(template: cflow.Template): Unit = {
    cache += (template.number -> template)
  }


  def one(packet: FlowPacket): Unit = {
    packet.flows.map(flow => {
      stream.queue.add(flow.toMap)
    })
  }

  override def handler(sender: InetSocketAddress, buf: ByteBuf): Unit = {


    Try {
      //buf.toString(Charset.defaultCharset())

      buf.getUnsignedShort(0)
    }.toOption.foreach {
      case 1 =>
        cflow.NetFlowV1Packet(sender, buf).toOption.foreach(one)
      case 5 =>
        cflow.NetFlowV5Packet(sender, buf).toOption.foreach(one)
      case 6 =>
        cflow.NetFlowV6Packet(sender, buf).toOption.foreach(one)
      case 7 =>
        cflow.NetFlowV7Packet(sender, buf).toOption.foreach(one)
      case 9 =>
        cflow.NetFlowV9Packet(sender, buf, this).toOption.foreach(one)
      case 10 =>
        val data = cflow.NetFlowIPFIXPacket(sender, buf, this).setHeaders
        logger.debug(s" net flow receive  $data")
        data.flatMap(_.dataRecords).map(flow => {
          stream.queue.add(flow.toMap)
        })
      case obj =>
        logger.warn(s"We do not handle none of NetFlow :$obj")
    }

    buf.release()
    /* handled.foreach(_.flows.map(flow => {
     //  logger.debug(s"flow.map:${flow.toMap}")
       stream.queue.add(flow.toMap)
     }))*/
  }
}




