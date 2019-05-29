package com.haima.sage.bigdata.etl.stream

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.charset.Charset

import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{RichMap, Stream}
import com.haima.sage.bigdata.etl.server.{SocketServer, TemplateProc}
import com.haima.sage.bigdata.etl.utils.Logger
import io.netflow.flows.cflow
import io.netflow.netty.TrafficHandler
import io.netty.buffer.ByteBuf

import scala.util.Try

/**
  * Created by zhhuiyan on 16/5/10.
  */
/**
  * Created by zhhuiyan on 2014/11/5.
  */
object JFlowStream {
  def apply(host: String = "0.0.0.0", port: Int = 2025, cacheSize: Int, _timeout: Long): Stream[RichMap] = new JFlowStream(host, port, _timeout)


}

class JFlowStream(host: String = "0.0.0.0", port: Int = 2025, _timeout: Long) extends QueueStream[RichMap](None, _timeout) {
  implicit val timeout = Timeout(5, java.util.concurrent.TimeUnit.MINUTES)


  val Client = new JFlowClient(this)


  var count = 0l


  @throws(classOf[IOException])
  override def close() {
    super.close()
    finished()

  }

  def getCache = CACHE_SIZE.toInt + 1

  def getHost = host

  def getPort = port


}

class JFlowClient(stream: JFlowStream) extends TrafficHandler with TemplateProc[cflow.Template] with Logger {

  val socket = SocketServer(stream.getHost, stream.getPort, stream.getCache * 1024, Some(this))
  socket.start()

  private var cache: Map[Int, cflow.Template] = Map()

  def templates = cache

  def setTemplate(template: cflow.Template): Unit = {
    cache += (template.number -> template)
  }


  override def handler(sender: InetSocketAddress, buf: ByteBuf): Unit = {
    {


      Try {
        buf.toString(Charset.defaultCharset())

        buf.getUnsignedShort(0)
      }.toOption match {
        case Some(1) =>
          val try1 = cflow.NetFlowV1Packet(sender, buf)
          try1.toOption
        case Some(5) =>
          cflow.NetFlowV5Packet(sender, buf).toOption.foreach(_.flows.map(flow => {
            //  logger.debug(s"flow.map:${flow.toMap}")
            stream.queue.add(flow.toMap)
          }))
        case Some(6) =>
          cflow.NetFlowV6Packet(sender, buf).toOption.foreach(_.flows.map(flow => {
            //  logger.debug(s"flow.map:${flow.toMap}")
            stream.queue.add(flow.toMap)
          }))
        case Some(7) =>
          cflow.NetFlowV7Packet(sender, buf).toOption.foreach(_.flows.map(flow => {
            //  logger.debug(s"flow.map:${flow.toMap}")
            stream.queue.add(flow.toMap)
          }))
        case Some(9) =>
          cflow.NetFlowV9Packet(sender, buf, this).toOption.foreach(_.flows.map(flow => {
            //  logger.debug(s"flow.map:${flow.toMap}")
            stream.queue.add(flow.toMap)
          }))
        case Some(10) =>
          val d = cflow.NetFlowV9Packet(sender, buf, this).toOption
          logger.debug(s"flow.map:$d")
          cflow.NetFlowV9Packet(sender, buf, this).toOption.foreach(_.flows.map(flow => {

            stream.queue.add(flow.toMap)
          }))
          val data = cflow.NetFlowIPFIXPacket(sender, buf, this).setHeaders
          logger.debug(s" net flow receive  ${data}")
          data.flatMap(_.dataRecords).map(flow => {
            stream.queue.add(flow.toMap)
          })
        case obj =>
          logger.warn(s"We do not handle none of NetFlow :${obj}")
      }
    }
    buf.release()
    /* handled.foreach(_.flows.map(flow => {
     //  logger.debug(s"flow.map:${flow.toMap}")
       stream.queue.add(flow.toMap)
     }))*/
  }
}

