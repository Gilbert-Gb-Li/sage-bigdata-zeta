package io.netflow
package flows.cflow

import java.net.{InetAddress, InetSocketAddress}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.server.TemplateProc
import com.haima.sage.bigdata.etl.utils.Logger
import io.netflow.flows.cflow.TemplateFields._
import io.netflow.lib._
import io.netty.buffer._
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

/**
  * NetFlow Version 9 Packet - FlowSet DataSet
  * https://www.plixer.com/support/netflow_v9.html
  * *-------*---------------*------------------------------------------------------*
  * | Bytes | Contents      | Description                                          |
  * *-------*---------------*------------------------------------------------------*
  * | 0-1   | version       | The version of NetFlow records exported 009          |
  * *-------*---------------*------------------------------------------------------*
  * | 2-3   | count         | Number of flows exported in this packet (1-30)       |
  * *-------*---------------*------------------------------------------------------*
  * | 4-7   | SysUptime     | Current time in milli since the export device booted |
  * *-------*---------------*------------------------------------------------------*
  * | 8-11  | unix_secs     | Current count of seconds since 0000 UTC 1970         |
  * *-------*---------------*------------------------------------------------------*
  * | 12-15 | PackageSeq    | Sequence counter of total flows exported             |
  * *-------*---------------*------------------------------------------------------*
  * | 16-19 | Source ID     | engine type+engine id                                |
  * *-------*---------------*------------------------------------------------------*
  * | 20-   | others        | Unused (zero) bytes                                  |
  * *-------*---------------*------------------------------------------------------*
  */
object NetFlowV9Packet extends Logger {
  private final val format = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd-HH-mmss")
    }
  }
  private val headerSize = 20

  // private def parseExtraFields = NodeConfig.values.netflow.extraFields

  /**
    * Parse a v9 Flow Packet
    *
    * @param sender The sender's InetSocketAddress
    * @param buf    Netty ByteBuf containing the UDP Packet
    * @param proc   Actor which holds the Templates for async saves
    */
  def apply(sender: InetSocketAddress, buf: ByteBuf, proc: TemplateProc[Template]): Try[NetFlowV9Packet] = Try[NetFlowV9Packet] {
    val length = buf.readableBytes()
    val version = buf.getUnsignedInteger(0, 2).toInt
    if (version != 9) return Failure(new InvalidFlowVersionException(version))

    val senderIP = sender.getAddress.getHostAddress
    val senderPort = sender.getPort
    if (length < headerSize)
      return Failure(new IncompleteFlowPacketHeaderException)

    val count = buf.getUnsignedInteger(2, 2).toInt
    val uptime = buf.getUnsignedInteger(4, 4)
    val timestamp = new DateTime(buf.getUnsignedInteger(8, 4) * 1000)
    val id = UUID.fromString(format.get().format(timestamp.toDate))
    val flowSequence = buf.getUnsignedInteger(12, 4)
    val sourceId = buf.getUnsignedInteger(16, 4)

    var flowsetCounter = 0
    var packetOffset = headerSize

    // we use a mutable array here in order not to bash the garbage collector so badly
    // because whenever we append something to our vector, the old vectors need to get GC'd
    val flows = scala.collection.mutable.ArrayBuffer[Flow[_]]()
    while (flowsetCounter < count && packetOffset < length) {
      val flowsetId = buf.getUnsignedInteger(packetOffset, 2).toInt
      val flowsetLength = buf.getUnsignedInteger(packetOffset + 2, 2).toInt
      if (flowsetLength == 0) return Failure(new IllegalFlowSetLengthException)
      if (packetOffset + flowsetLength > length) return Failure(new ShortFlowPacketException)

      flowsetId match {
        case 0 | 2 => // template flowset - 0 NetFlow v9, 2 IPFIX
          var templateOffset = packetOffset + 4 // add the 4 byte flowset Header
          logger.debug("Template FlowSet (" + flowsetId + ") from " + senderIP + ":" + senderPort)
          do {
            val fieldCount = buf.getUnsignedShort(templateOffset + 2)
            val templateSize = fieldCount * 4 + 4
            if (templateOffset + templateSize < length) {
              val buffer = buf.slice(templateOffset, templateSize)
              NetFlowV9Template(sender, buffer, id, flowsetId, timestamp) match {
                case Success(tmpl) =>
                  proc.setTemplate(tmpl)
                //    flows += tmpl
                case Failure(e) => logger.warn(e.toString)
              }
              //  flowsetCounter += 1
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case 1 | 3 => // template flowset - 1 NetFlow v9, 3 IPFIX
          logger.debug("OptionTemplate FlowSet (" + flowsetId + ") from " + senderIP + ":" + senderPort)
          var templateOffset = packetOffset + 4 // add the 4 byte flowset Header
          do {
            val scopeLen = buf.getUnsignedInteger(templateOffset + 2, 2).toInt
            val optionLen = buf.getUnsignedInteger(templateOffset + 4, 2).toInt
            val templateSize = scopeLen + optionLen + 6
            if (templateOffset + templateSize < length) {
              val buffer = buf.slice(templateOffset, templateSize)
              NetFlowV9Template(sender, buffer, id, flowsetId, timestamp) match {
                case Success(tmpl) =>
                  proc.setTemplate(tmpl)
                // flows += tmpl
                case Failure(e) => logger.warn(e.toString); e.printStackTrace()
              }
              //   flowsetCounter += 1
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case a: Int if a > 255 => // flowset - templateId == flowsetId
          proc.templates.get(flowsetId).
            filter(_.isInstanceOf[NetFlowV9Template]).
            map(_.asInstanceOf[NetFlowV9Template]).
            foreach { tmpl =>
              val option = tmpl.flowsetId == 1
              var recordOffset = packetOffset + 4 // add the 4 byte flowset Header
              while (recordOffset - packetOffset + tmpl.length <= flowsetLength) {
                val buffer = buf.slice(recordOffset, tmpl.length)
                val flow =
                  if (option) optionRecord(sender, buffer, id, tmpl, uptime, timestamp)
                  else dataRecord(sender, buffer, id, tmpl, uptime, timestamp)

                flow match {
                  case Success(_flow) => flows += _flow
                  case Failure(e) => logger.warn(e.toString)
                }
                flowsetCounter += 1
                recordOffset += tmpl.length
              }
            }
        case a: Int => logger.debug("Unexpected TemplateId (" + a + ")")
      }
      packetOffset += flowsetLength
    }

    NetFlowV9Packet(id, sender, length, uptime, new Date(timestamp.getMillis), flows.toList, flowSequence, sourceId)
  }

  /**
    * Parse a Version 9 Flow
    *
    * @param sender    The sender's InetSocketAddress
    * @param buf       Netty ByteBuf containing the UDP Packet
    * @param fpId      FlowPacket-UUID this Flow arrived in
    * @param template  NetFlow Template for this Flow
    * @param timestamp DateTime when this flow was exported
    */
  def dataRecord(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, template: NetFlowV9Template,
                 uptime: Long, timestamp: DateTime) = Try[NetFlowV9Data] {
    val srcPort = buf.getUnsignedInteger(template, L4_SRC_PORT).get.toInt
    val dstPort = buf.getUnsignedInteger(template, L4_DST_PORT).get.toInt

    val srcAS = buf.getUnsignedInteger(template, SRC_AS).map(_.toInt).filter(_ != -1)
    val dstAS = buf.getUnsignedInteger(template, DST_AS).map(_.toInt).filter(_ != -1)
    val proto = (buf.getUnsignedInteger(template, PROT) getOrElse -1L).toInt
    val tos = (buf.getUnsignedInteger(template, SRC_TOS) getOrElse -1L).toInt

    // calculate the offset from uptime and subtract that from the timestamp
    val start = buf.getUnsignedInteger(template, FIRST_SWITCHED).filter(_ == 0).map(x => new Date(timestamp.minus(uptime - x).getMillis))
    val stop = buf.getUnsignedInteger(template, LAST_SWITCHED).filter(_ == 0).map(x => new Date(timestamp.minus(uptime - x).getMillis))
    val tcpflags = (buf.getUnsignedInteger(template, TCP_FLAGS) getOrElse -1L).toInt

    val srcAddress = buf.getInetAddress(template, IPV4_SRC_ADDR, IPV6_SRC_ADDR)
    val dstAddress = buf.getInetAddress(template, IPV4_DST_ADDR, IPV6_DST_ADDR)
    val nextHop = Option(buf.getInetAddress(template, IPV4_NEXT_HOP, IPV6_NEXT_HOP)).
      filter(_.getHostAddress != "0.0.0.0") // FIXME filter v6

    val pkts = buf.getUnsignedInteger(template, InPKTS, OutPKTS).get
    val bytes = buf.getUnsignedInteger(template, InBYTES, OutBYTES).get
    val extraFields: Map[String, Long] = template.getExtraFields(buf)
    NetFlowV9Data(UUID.randomUUID(), sender, buf.readableBytes(), template.number, uptime, new Date(timestamp.getMillis),
      srcPort, dstPort, srcAS, dstAS, pkts, bytes, proto,
      tos, tcpflags, start, stop, srcAddress, dstAddress, nextHop, extraFields, fpId)
  }

  /**
    * Parse a Version 9 Option Flow
    *
    * @param sender    The sender's InetSocketAddress
    * @param buf       Netty ByteBuf containing the UDP Packet
    * @param fpId      FlowPacket-UUID which this Flow arrived in
    * @param template  NetFlow Template for this Flow
    * @param timestamp DateTime when this flow was exported
    */
  def optionRecord(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, template: NetFlowV9Template,
                   uptime: Long, timestamp: DateTime) = Try[NetFlowV9Option] {
    NetFlowV9Option(UUID.randomUUID(), sender, buf.readableBytes(), template.number, uptime, timestamp,
      template.getExtraFields(buf), fpId)
  }


}

case class NetFlowV9Packet(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long,
                           timestamp: Date, flows: List[Flow[_]],
                           flowSequence: Long, sourceId: Long) extends FlowPacket {
  def version = "NetFlowV9 Packet"

  def count = flows.length
}

case class NetFlowV9Data(id: UUID, sender: InetSocketAddress, length: Int, template: Int, uptime: Long,
                         timestamp: Date, srcPort: Int, dstPort: Int, srcAS: Option[Int], dstAS: Option[Int],
                         pkts: Long, bytes: Long, proto: Int, tos: Int, tcpflags: Int,
                         start: Option[Date], stop: Option[Date],
                         srcAddress: InetAddress, dstAddress: InetAddress, nextHop: Option[InetAddress],
                         extra: Map[String, Long], packet: UUID) extends NetFlowData[NetFlowV9Data] {
  def version = "NetFlowV9Data " + template

  override lazy val stringExtra = "- Template " + template

  override def toMap: Map[String, Any] =
    Map("id" -> id,
      "host" -> sender.getHostString,
      "port" -> sender.getPort,
      "length" -> length,
      "template" -> template,
      "uptime" -> uptime,
      "@timestamp" -> timestamp,
      "src" -> srcAddress.getHostName,
      "srcAS" -> srcAS.getOrElse(-1),
      "src_port" -> srcPort,
      "dst_port" -> dstPort,
      "dstAs" -> dstAS.getOrElse(-1),
      "dst" -> dstAddress.getHostName,
      "pkts" -> pkts,
      "proto" -> proto,
      "tos" -> tos,
      "tcpflags" -> tcpflags,
      "start" -> start.orNull,
      "stop" -> stop.orNull,
      "bytes" -> bytes,
      "packet" -> packet,
      "version" -> version
    ) ++ extra
}

case class NetFlowV9Option(id: UUID, sender: InetSocketAddress, length: Int, template: Int, uptime: Long,
                           timestamp: DateTime, extra: Map[String, Long], packet: UUID)
  extends Flow[NetFlowV9Option] {
  def version = "NetFlowV9Option " + template

  override def toMap: Map[String, Any] =
    Map("id" -> id,
      "host" -> sender.getHostString,
      "port" -> sender.getPort,
      "length" -> length,
      "template" -> template,
      "uptime" -> new Date(uptime),
      "@timestamp" -> timestamp,
      "packet" -> packet,
      "version" -> version
    ) ++ extra
}

