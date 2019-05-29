package io.netflow.flows.sflow

import java.net.{InetAddress, InetSocketAddress}
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.utils.Logger
import io.netflow.flows.cflow.HeaderParseException
import io.netflow.lib._
import io.netty.buffer._

import scala.util.{Failure, Try}

/**
  * sFlow Version 5 Packet
  * http://www.sflow.org/developers/diagrams/sFlowV5FlowData.pdf
  * On sFlows which don't have srcAS and dstAS, we simply set them to 0.
  *
  * Flow coming from a v4 Agent
  *
  * *-------*---------------*------------------------------------------------------*
  * | Bytes | Contents      | Description                                          |
  * *-------*---------------*------------------------------------------------------*
  * | 0-3   | version       | The version of sFlow records exported 005            |
  * *-------*---------------*------------------------------------------------------*
  * | 4-7   | agent version | The InetAddress version of the Agent (1=v4, 2=v6)    |
  * *-------*---------------*------------------------------------------------------*
  * | 8-11  | agent IPv4    | IPv4 Address of the Agent                            |
  * *-------*---------------*------------------------------------------------------*
  * | 12-15 | agent sub id  | Agent Sub ID                                         |
  * *-------*---------------*------------------------------------------------------*
  * | 16-19 | sequence id   | Sequence counter of total flows exported             |
  * *-------*---------------*------------------------------------------------------*
  * | 20-23 | sysuptime     | System Uptime                                        |
  * *-------*---------------*------------------------------------------------------*
  * | 24-27 | samples       | Number of samples                                    |
  * *-------*---------------*------------------------------------------------------*
  *
  * Flow coming from a v6 Agent
  *
  * *-------*---------------*------------------------------------------------------*
  * | Bytes | Contents      | Description                                          |
  * *-------*---------------*------------------------------------------------------*
  * | 0-3   | version       | The version of sFlow records exported 005            |
  * *-------*---------------*------------------------------------------------------*
  * | 4-7   | agent version | The InetAddress version of the Agent (1=v4, 2=v6)    |
  * *-------*---------------*------------------------------------------------------*
  * | 8-23  | agent IPv6    | IPv6 Address of the Agent                            |
  * *-------*---------------*------------------------------------------------------*
  * | 24-27 | agent sub id  | Agent Sub ID                                         |
  * *-------*---------------*------------------------------------------------------*
  * | 28-31 | sequence id   | Sequence counter of total flows exported             |
  * *-------*---------------*------------------------------------------------------*
  * | 32-35 | sysuptime     | System Uptime                                        |
  * *-------*---------------*------------------------------------------------------*
  * | 36-39 | samples       | Number of samples                                    |
  * *-------*---------------*------------------------------------------------------*
  */

object SFlowV5Packet extends Logger {

  /**
    * Parse a Version 5 sFlow Packet
    *
    * @param sender The sender's InetSocketAddress
    * @param buf    Netty ByteBuf containing the UDP Packet
    */
  def apply(sender: InetSocketAddress, buf: ByteBuf): Try[SFlowV5Packet] = Try[SFlowV5Packet] {
    val version = buf.getUnsignedInteger(0, 4).toInt
    if (version != 5) return Failure(new InvalidFlowVersionException(version))

    buf.readerIndex(0)

    if (buf.readableBytes < 28)
      return Failure(new IncompleteFlowPacketHeaderException)

    val agentIPversion = if (buf.getUnsignedInteger(4, 4) == 1L) 4 else 6
    val agentLength = if (agentIPversion == 4) 4 else 16
    val agent = buf.getInetAddress(8, agentLength)

    var offset = 8 + agentLength
    val agentSubId = buf.getUnsignedInteger(offset, 4)
    val sequenceId = buf.getUnsignedInteger(offset + 4, 4)
    val uptime = buf.getUnsignedInteger(offset + 8, 4)
    val count = buf.getUnsignedInteger(offset + 12, 4).toInt
    val id = UUID.randomUUID()
    offset = offset + 16

    val flows: List[SampleDataHeader] = (0 to count).map(i =>{
      val flow = apply(5, sender, buf.slice(offset, buf.readableBytes - offset), id)
      offset += flow.length.toInt + 8
      flow}) .toList
    //val flows: List[SFlowV5] = List()
    SFlowV5Packet(id, sender, buf.readableBytes, agent, agentSubId, sequenceId, uptime, flows)
  }

  /**
    * Parse a Version 5 sFlow
    *
    */
  def apply(version: Int, sender: InetSocketAddress, buf: ByteBuf, fpId: UUID): SampleDataHeader = {
    SampleDataHeader(UUID.randomUUID(),sender,buf)
  }
}

case class SFlowV5Packet(id: UUID, sender: InetSocketAddress, length: Int, agent: InetAddress,
                         agentSubId: Long, sequenceId: Long, uptime: Long, flows: List[SampleDataHeader]) extends FlowPacket {
  def version = "sFlowV5 Packet"

  def count = flows.length

  lazy val timestamp = new Date()
}

object SampleDataHeader extends Logger{
  val EXPANDEDFLOWSAMPLE: Long = 3
  val EXPANDEDCOUNTERSAMPLE: Long = 4

  @throws[HeaderParseException]
  def apply( id: UUID, sender: InetSocketAddress,data: ByteBuf): SampleDataHeader = {
    try {
      if (data.readableBytes() < 8) throw new HeaderParseException("Data array too short.")
      val format = data.getUnsignedInteger(0, 4)

      val length = data.getUnsignedInteger(4, 4).toInt


      val fs = if (format == EXPANDEDFLOWSAMPLE) {
        ExpandedFlowSampleHeader.parse(data.copy(8, data.readableBytes() - length.toInt))
      } else {
        null
      }
      val fcs = if (format == EXPANDEDCOUNTERSAMPLE) {
        ExpandedCounterSampleHeader.parse(data.copy(8, data.readableBytes() - length.toInt))
      } else {
        null
      }

      if ((format != EXPANDEDFLOWSAMPLE) && (format != EXPANDEDCOUNTERSAMPLE)) {
        logger.error(s"Sample data format not yet supported: $format")
      }
      SampleDataHeader(id,sender,format, length, fs, fcs)
    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

case class SampleDataHeader(override val id: UUID = UUID.randomUUID(), override val sender: InetSocketAddress, sampleDataFormat: Long = 0L, override val length: Int = 0,
                            expandedFlowSampleHeader: ExpandedFlowSampleHeader = null,
                            expandedCounterSampleHeader: ExpandedCounterSampleHeader = null) extends Flow[SampleDataHeader] {

  override def version: String = "sFlowV5 Flow"

  override def toMap: Map[String, Any] = Map(
    "id" -> id.toString,
    "host" -> sender.getHostString,
    "port" -> sender.getPort,
    "format" -> sampleDataFormat,
    "length" -> length
  )


}

object ExpandedFlowSampleHeader {
  @throws[HeaderParseException]
  def parse(data: ByteBuf): ExpandedFlowSampleHeader = {
    try {
      if (data.readableBytes() < 44) throw new HeaderParseException("Data array too short.")
      val seqNumber = data.getUnsignedInteger(0, 4)
      val sourceIDType = data.getUnsignedInteger(4, 4)
      val sourceIDIndex = data.getUnsignedInteger(8, 4)
      val samplingRate = data.getUnsignedInteger(12, 4)
      val samplePool = data.getUnsignedInteger(16, 4)
      val drops = data.getUnsignedInteger(20, 4)
      val inputInterfaceFormat = data.getUnsignedInteger(24, 4)
      val inputInterfaceValue = data.getUnsignedInteger(28, 4)
      val outputInterfaceFormat = data.getUnsignedInteger(32, 4)
      val outputInterfaceValue = data.getUnsignedInteger(36, 4)
      val numberFlowRecords = data.getUnsignedInteger(40, 4)
      var offset: Int = 44

      ExpandedFlowSampleHeader(seqNumber, sourceIDType, sourceIDIndex, samplingRate, samplePool, drops, inputInterfaceFormat, inputInterfaceValue,
        outputInterfaceFormat, outputInterfaceValue, numberFlowRecords, (0 to numberFlowRecords.toInt).map(i => {
          val subData = data.copy(offset, data.readableBytes() - offset)
          val fr: FlowRecordHeader = FlowRecordHeader(subData)

          offset += (fr.flowDataLength.toInt + 8)
          fr
        }).toSet)


    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

case class ExpandedFlowSampleHeader(seqNumber: Long = 0L,
                                    sourceIDType: Long = 0L,
                                    sourceIDIndex: Long = 0L,
                                    samplingRate: Long = 0L,
                                    samplePool: Long = 0L,
                                    drops: Long = 0L,
                                    inputInterfaceFormat: Long = 0L,
                                    inputInterfaceValue: Long = 0L,
                                    outputInterfaceFormat: Long = 0L,
                                    outputInterfaceValue: Long = 0L,
                                    numberFlowRecords: Long = 0L,
                                    flowRecords: Set[FlowRecordHeader] = null) {}

object ExpandedCounterSampleHeader {
  @throws[HeaderParseException]
  def parse(data: ByteBuf): ExpandedCounterSampleHeader = {
    try {
      if (data.readableBytes() < 16) throw new HeaderParseException("Data array too short.")
      val seqNumber = data.getUnsignedInteger(0, 4)
      val sourceIDType = data.getUnsignedInteger(4, 4)
      val sourceIDIndex = data.getUnsignedInteger(8, 4)
      val numberCounterRecords = data.getUnsignedInteger(12, 4)
      var offset: Int = 16
      var i: Int = 0

      ExpandedCounterSampleHeader(seqNumber, sourceIDType, sourceIDIndex, numberCounterRecords, (0 to numberCounterRecords.toInt).map(i => {
        val subData = data.copy(offset, data.readableBytes() - offset)
        val cr = CounterRecordHeader.parse(subData)

        offset += (cr.counterDataLength.toInt + 8)
        cr
      }).toSet)
    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

case class ExpandedCounterSampleHeader(seqNumber: Long = 0L,
                                       sourceIDType: Long = 0L,
                                       sourceIDIndex: Long = 0L,
                                       numberCounterRecords: Long = 0L,
                                       counterRecords: Set[CounterRecordHeader] = null) {}

object FlowRecordHeader {
  @throws[HeaderParseException]
  def apply(data: ByteBuf): FlowRecordHeader = {
    try {
      if (data.readableBytes() < 8) throw new HeaderParseException("Data array too short.")
      val frd: FlowRecordHeader = new FlowRecordHeader
      val format = data.getUnsignedInteger(0, 4)
      val length = data.getUnsignedInteger(4, 4)
      val subData = data.copy(8, length.toInt)
      val rp: RawPacketHeader = RawPacketHeader(subData)

      FlowRecordHeader(format, length, rp)
    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

case class FlowRecordHeader(flowDataFormat: Long = 0L, flowDataLength: Long = 0L, rawPacket: RawPacketHeader = null) {

}

object RawPacketHeader extends Logger{
  val ETHERNET_ISO88023: Int = 1
  val TOKENBUS_ISO88024: Int = 2
  val TOKENRING_ISO88025: Int = 3
  val FDDI: Int = 4
  val FRAME_RELAY: Int = 5
  val X25: Int = 6
  val PPP: Int = 7
  val SMDS: Int = 8
  val AAL5: Int = 9
  val AAL5_IP: Int = 10
  val IPV4: Int = 11
  val IPV6: Int = 12
  val MPLS: Int = 13
  val POS: Int = 14

  @throws[HeaderParseException]
  def apply(data: ByteBuf): RawPacketHeader = {
    try {
      if (data.readableBytes() < 16) throw new HeaderParseException("Data array too short.")
      val rp: RawPacketHeader = new RawPacketHeader
      val headerProtocol = data.getUnsignedInteger(0, 4)
      val frameLength = data.getUnsignedInteger(4, 4)
      val stripped = data.getUnsignedInteger(8, 4)
      val headerSize = data.getUnsignedInteger(12, 4)
      if (headerProtocol == ETHERNET_ISO88023) {
        new RawPacketHeader(headerProtocol, frameLength, stripped, headerSize, MacHeader(data.copy(16, data.readableBytes() - 16)))
      }
      else {
        logger.error(s"Sample data format not yet supported: $headerProtocol")
        new RawPacketHeader(headerProtocol, frameLength, stripped, headerSize)
      }
    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

class RawPacketHeader(headerProtocol: Long = 0L, frameLength: Long = 0L, stripped: Long = 0L, headerSize: Long = 0L, macHeader: MacHeader = null) {

}

object MacHeader {
  def apply(data: ByteBuf): MacHeader = {
    try {
      if (data.readableBytes() < 14) throw new HeaderParseException("Data array too short.")
      val destination = data.getUnsignedInteger(0, 6)
      val source = data.getUnsignedInteger(6, 6)



      val offcut = data.copy(14, data.readableBytes() - 14)


      if ((data.getByte(12) == (0x81 & 0xFF).toByte) && (data.getByte(13) == (0x00 & 0xFF).toByte)) {
        val tpid = 0x81 << 8 + 0x00
        val tci = data.getUnsignedInteger(14, 2).toInt


        new TaggedMacHeader(tpid, tci, destination, source, data.getUnsignedInteger(16, 2).toInt, data.copy(18, data.readableBytes() - 14))
      } else {
        val `type` = data.getUnsignedInteger(12, 2).toInt

        new MacHeader(destination, source, `type`, data.copy(14, data.readableBytes() - 14))
      }

    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

class MacHeader(destination: Long = 0L, source: Long = 0L, `type`: Int = 0, offcut: ByteBuf) {}

case class TaggedMacHeader(tpid: Int, tci: Int, destination: Long = 0L, source: Long = 0L, `type`: Int = 0, offcut: ByteBuf)
  extends MacHeader(destination, source, `type`, offcut) {}

object CounterRecordHeader extends Logger{
  val GENERICINTERFACECOUNTER: Int = 1
  val ETHERNETINTERFACECOUNTER: Int = 2

  @throws[HeaderParseException]
  def parse(data: ByteBuf): CounterRecordHeader = {
    try {
      if (data.readableBytes() < 8) throw new HeaderParseException("Data array too short.")
      val format = data.getUnsignedInteger(0, 4)

      val length = data.getUnsignedInteger(4, 4)
      val subData = data.copy(8, length.toInt)
      val gic = if (format == GENERICINTERFACECOUNTER) {
        GenericInterfaceCounterHeader.parse(subData)
      } else {
        null
      }
      val eic: EthernetInterfaceCounterHeader = if (format == ETHERNETINTERFACECOUNTER) {
        EthernetInterfaceCounterHeader.parse(subData)
      } else {
        null
      }
      if ((format != GENERICINTERFACECOUNTER) && (format != ETHERNETINTERFACECOUNTER)) {
        logger.error(s"Counter data format not yet supported: $format")
      }
      CounterRecordHeader(format, length, gic, eic)
    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

case class CounterRecordHeader(
                                counterDataFormat: Long = 0L,
                                counterDataLength: Long = 0L,
                                genericInterfaceCounter: GenericInterfaceCounterHeader = null,
                                ethernetInterfaceCounter: EthernetInterfaceCounterHeader = null) {}

object GenericInterfaceCounterHeader {
  @throws[HeaderParseException]
  def parse(data: ByteBuf): GenericInterfaceCounterHeader = {
    try {
      if (data.readableBytes() < 88) throw new HeaderParseException("Data array too short.")
      val ifIndex = data.getUnsignedInteger(0, 4)
      val ifType = data.getUnsignedInteger(4, 4)
      val ifSpeed = data.getUnsignedInteger(8, 8)

      val ifDirection = data.getUnsignedInteger(16, 4)
      val ifStatus = data.getUnsignedInteger(20, 4)
      val ifInOctets = data.getUnsignedInteger(24, 8)
      val ifInUcastPkts = data.getUnsignedInteger(32, 4)
      val ifInMulticastPkts = data.getUnsignedInteger(36, 4)
      val ifInBroadcastPkts = data.getUnsignedInteger(40, 4)
      val ifInDiscards = data.getUnsignedInteger(44, 4)
      val ifInErrors = data.getUnsignedInteger(48, 4)
      val ifInUnknownProtos = data.getUnsignedInteger(52, 4)
      val ifOutOctets = data.getUnsignedInteger(56, 8)
      val ifOutUcastPkts = data.getUnsignedInteger(64, 4)
      val ifOutMulticastPkts = data.getUnsignedInteger(68, 4)
      val ifOutBroadcastPkts = data.getUnsignedInteger(72, 4)
      val ifOutDiscards = data.getUnsignedInteger(76, 4)

      val ifOutErrors = data.getUnsignedInteger(80, 4)
      val ifPromiscuousMode = data.getUnsignedInteger(84, 4)
      val subData = data.copy(88, data.readableBytes() - 88)
      val cd: CounterData = CounterData(subData)
      GenericInterfaceCounterHeader(ifIndex, ifType, ifSpeed, ifDirection, ifStatus, ifInOctets, ifInUcastPkts, ifInMulticastPkts, ifInBroadcastPkts, ifInDiscards, ifInErrors,
        ifInUnknownProtos, ifOutOctets, ifOutUcastPkts, ifOutMulticastPkts, ifOutBroadcastPkts, ifOutDiscards, ifOutErrors, ifPromiscuousMode, cd
      )
    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

case class GenericInterfaceCounterHeader(ifIndex: Long = 0L,
                                         ifType: Long = 0L,
                                         ifSpeed: Long = 0,
                                         ifDirection: Long = 0L,
                                         ifStatus: Long = 0L,
                                         ifInOctets: Long = 0,
                                         ifInUcastPkts: Long = 0L,
                                         ifInMulticastPkts: Long = 0L,
                                         ifInBroadcastPkts: Long = 0L,
                                         ifInDiscards: Long = 0L,
                                         ifInErrors: Long = 0L,
                                         ifInUnknownProtos: Long = 0L,
                                         ifOutOctets: Long = 0,
                                         ifOutUcastPkts: Long = 0L,
                                         ifOutMulticastPkts: Long = 0L,
                                         ifOutBroadcastPkts: Long = 0L,
                                         ifOutDiscards: Long = 0L,
                                         ifOutErrors: Long = 0L,
                                         ifPromiscuousMode: Long = 0L,
                                         counterData: CounterData = null) {


}

object EthernetInterfaceCounterHeader {
  @throws[HeaderParseException]
  def parse(data: ByteBuf): EthernetInterfaceCounterHeader = {
    try {
      if (data.readableBytes() < 52) throw new HeaderParseException("Data array too short.")
      val dot3StatsAlignmentErrors = data.getUnsignedInteger(0, 4)
      val dot3StatsFCSErrors = data.getUnsignedInteger(4, 4)
      val dot3StatsSingleCollisionFrames = data.getUnsignedInteger(8, 4)
      val dot3StatsMultipleCollisionFrames = data.getUnsignedInteger(12, 4)
      val dot3StatsSQETestErrors = data.getUnsignedInteger(16, 4)
      val dot3StatsDeferredTransmissions = data.getUnsignedInteger(20, 4)
      val dot3StatsLateCollisions = data.getUnsignedInteger(24, 4)
      val dot3StatsExcessiveCollisions = data.getUnsignedInteger(28, 4)
      val dot3StatsInternalMacTransmitErrors = data.getUnsignedInteger(32, 4)
      val dot3StatsCarrierSenseErrors = data.getUnsignedInteger(36, 4)
      val dot3StatsFrameTooLongs = data.getUnsignedInteger(40, 4)
      val dot3StatsInternalMacReceiveErrors = data.getUnsignedInteger(44, 4)
      val dot3StatsSymbolErrors = data.getUnsignedInteger(48, 4)
      val subData = data.copy(52, data.readableBytes() - 52)
      val cd: CounterData = CounterData(subData)
      EthernetInterfaceCounterHeader(dot3StatsAlignmentErrors, dot3StatsFCSErrors, dot3StatsSingleCollisionFrames
        , dot3StatsMultipleCollisionFrames, dot3StatsSQETestErrors, dot3StatsDeferredTransmissions, dot3StatsLateCollisions,
        dot3StatsExcessiveCollisions, dot3StatsInternalMacTransmitErrors, dot3StatsCarrierSenseErrors, dot3StatsFrameTooLongs, dot3StatsInternalMacReceiveErrors, dot3StatsSymbolErrors, counterData = cd)
    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

case class EthernetInterfaceCounterHeader(
                                           dot3StatsAlignmentErrors: Long = 0L,
                                           dot3StatsFCSErrors: Long = 0L,
                                           dot3StatsSingleCollisionFrames: Long = 0L,
                                           dot3StatsMultipleCollisionFrames: Long = 0L,
                                           dot3StatsSQETestErrors: Long = 0L,
                                           dot3StatsDeferredTransmissions: Long = 0L,
                                           dot3StatsLateCollisions: Long = 0L,
                                           dot3StatsExcessiveCollisions: Long = 0L,
                                           dot3StatsInternalMacTransmitErrors: Long = 0L,
                                           dot3StatsCarrierSenseErrors: Long = 0L,
                                           dot3StatsFrameTooLongs: Long = 0L,
                                           dot3StatsInternalMacReceiveErrors: Long = 0L,
                                           dot3StatsSymbolErrors: Long = 0L,
                                           counterData: CounterData = null) {
}

case class CounterData(data: ByteBuf) {

}