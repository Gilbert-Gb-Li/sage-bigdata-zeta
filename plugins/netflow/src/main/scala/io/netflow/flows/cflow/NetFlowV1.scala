package io.netflow.flows.cflow

import java.net.{InetAddress, InetSocketAddress}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, UUID}

import io.netflow.lib._
import io.netty.buffer._
import org.joda.time.DateTime

import scala.util.{Failure, Try}

/**
  * NetFlow Version 1
  *
  * *-------*---------------*------------------------------------------------------*
  * | Bytes | Contents      | Description                                          |
  * *-------*---------------*------------------------------------------------------*
  * | 0-1   | version       | The version of NetFlow records exported 005          |
  * *-------*---------------*------------------------------------------------------*
  * | 2-3   | count         | Number of flows exported in this packet (1-30)       |
  * *-------*---------------*------------------------------------------------------*
  * | 4-7   | SysUptime     | Current time in milli since the export device booted |
  * *-------*---------------*------------------------------------------------------*
  * | 8-11  | unix_secs     | Current count of seconds since 0000 UTC 1970         |
  * *-------*---------------*------------------------------------------------------*
  * | 12-15 | unix_nsecs    | Residual nanoseconds since 0000 UTC 1970             |
  * *-------*---------------*------------------------------------------------------*
  */

object NetFlowV1Packet {
  private val headerSize = 16
  private val flowSize = 48
  private final val format = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd-HH-mmss")
    }
  }

  /**
    * Parse a Version 1 FlowPacket
    *
    * @param sender The sender's InetSocketAddress
    * @param buf    Netty ByteBuf containing the UDP Packet
    */
  def apply(sender: InetSocketAddress, buf: ByteBuf): Try[NetFlowV1Packet] = Try[NetFlowV1Packet] {
    val version = buf.getUnsignedInteger(0, 2).toInt
    if (version != 1) return Failure(new InvalidFlowVersionException(version))

    val count = buf.getUnsignedInteger(2, 2).toInt
    if (count <= 0 || buf.readableBytes < headerSize + count * flowSize)
      return Failure(new CorruptFlowPacketException)

    val uptime = buf.getUnsignedInteger(4, 4)
    val timestamp = new DateTime(buf.getUnsignedInteger(8, 4) * 1000)
    val id = UUID.fromString(format.get().format(timestamp.toDate))

    val flows: List[NetFlowV1] = (0 to count - 1).toList.flatMap { i =>
      apply(sender, buf.slice(headerSize + (i * flowSize), flowSize), id, uptime, timestamp)
    }
    NetFlowV1Packet(id, sender, buf.readableBytes, uptime, new Date(timestamp.getMillis), flows)
  }

  /**
    * Parse a Version 1 Flow
    *
    * @param sender    The sender's InetSocketAddress
    * @param buf       Netty ByteBuf Slice containing the UDP Packet
    * @param fpId      FlowPacket-UUID this Flow arrived on
    * @param uptime    Millis since UNIX Epoch when the exporting device/sender booted
    * @param timestamp DateTime when this flow was exported
    */
  def apply(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, uptime: Long, timestamp: DateTime): Option[NetFlowV1] =
    Try[NetFlowV1] {
      NetFlowV1(UUID.randomUUID(), sender, buf.readableBytes(), uptime, new Date(timestamp.getMillis),
        buf.getUnsignedInteger(32, 2).toInt, // srcPort
        buf.getUnsignedInteger(34, 2).toInt, // dstPort
        None, None, // srcAS and dstAS
        buf.getUnsignedInteger(16, 4), // pkts
        buf.getUnsignedInteger(20, 4), // bytes
        buf.getUnsignedByte(38).toInt, // proto
        buf.getUnsignedByte(39).toInt, // tos
        buf.getUnsignedByte(40).toInt, // tcpflags
        Some(buf.getUnsignedInteger(24, 4)).filter(_ != 0).map(x => new Date(timestamp.minus(uptime - x).getMillis)), // start
        Some(buf.getUnsignedInteger(28, 4)).filter(_ != 0).map(x => new Date(timestamp.minus(uptime - x).getMillis)), // stop
        buf.getInetAddress(0, 4), // srcAddress
        buf.getInetAddress(4, 4), // dstAddress
        Option(buf.getInetAddress(8, 4)).filter(_.getHostAddress != "0.0.0.0"), // nextHop
        buf.getUnsignedInteger(12, 2).toInt, // snmpInput
        buf.getUnsignedInteger(14, 2).toInt, // snmpOutput
        fpId)
    }.toOption

}

case class NetFlowV1Packet(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long, timestamp: Date,
                           flows: List[NetFlowV1]) extends FlowPacket {
  def version = "NetFlowV1 Packet"

  def count = flows.length
}

case class NetFlowV1(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long, timestamp: Date,
                     srcPort: Int, dstPort: Int, srcAS: Option[Int], dstAS: Option[Int],
                     pkts: Long, bytes: Long, proto: Int, tos: Int, tcpflags: Int,
                     start: Option[Date], stop: Option[Date],
                     srcAddress: InetAddress, dstAddress: InetAddress, nextHop: Option[InetAddress],
                     snmpInput: Int, snmpOutput: Int, packet: UUID) extends NetFlowData[NetFlowV1] {

  def version = "NetFlowV1"

  override def toMap: Map[String, Any] = Map("id" -> id,
    "host" -> sender.getHostString,

    "port" -> sender.getPort,
    "length" -> length,
    "template" -> sender.getPort,
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
    "start" -> start.get,
    "stop" -> stop.get,
    "bytes" -> bytes,
    "packet" -> packet,
    "snmpInput" -> snmpInput,
    "snmpOutput" -> snmpOutput,

    "version" -> version
  )
}
