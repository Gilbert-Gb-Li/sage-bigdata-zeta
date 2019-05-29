package io.netflow.flows.cflow

import java.math.BigInteger
import java.net._
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.server.TemplateProc
import com.haima.sage.bigdata.etl.utils.Logger
import io.netflow.flows.cflow
import io.netflow.lib._
import io.netty.buffer.ByteBuf

import scala.util.Try

/**
  * This file is part of jsFlow.
  *
  * Copyright (c) 2009 DE - CIX Management GmbH <http://www.de-cix.net>- All rights
  * reserved.
  *
  * Author:zhhuiyan
  * Created by zhhuiyan on 16/4/29.
  *
  * This information set would be sent in the following IPFIX message:
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |  Bits 0..15	                |            Bits 16..31            |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |  Version = 0x000a	          |       Message Length = 64 Bytes   |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |             Export Timestamp = 2005-12-31 23:59:60              |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |                   Sequence Number = 0                           |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |                 Observation Domain ID = 12345678                |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |   Set ID = 2 (Template)     |    Set Length = 20 Bytes          |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |      Template ID = 256	    |    Number of Fields = 3           |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |   Typ = sourceIPv4Address	  |   Field Length = 4 Bytes          |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |Typ = destinationIPv4Address |	Field Length = 4 Bytes            |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |      Typ = packetDeltaCount	|   Field Length = 4 Bytes          |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |    Set ID = 256 (Data Set   |     Set Length = 28 Bytes         |
  * +                             +                                   +
  * |      using Template 256)	  |                                   |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |             Record 1, Field 1 = 192.168.0.201                   |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |             Record 1, Field 2 = 192.168.0.1                     |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |             Record 1, Field 3 = 235 Packets                     |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |             Record 2, Field 1 = 192.168.0.202                   |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |             Record 2, Field 2 = 192.168.0.1                     |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  * |             Record 2, Field 3 = 42 Packets                      |
  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *
  */
trait IPFIXEntity {
  override def toString: String


}

abstract class Record extends IPFIXEntity {
  def length: Long
}

abstract class DataRecord() extends Record {
  def data: ByteBuf

  def toMap: Map[String, Any]

  override def length = data.readableBytes()
}


object DefaultDataRecord {
  def apply(data: ByteBuf, template: TemplateRecord): DefaultDataRecord = {
    var offset = 0
    new DefaultDataRecord(template.informationElements.map(in => {
      val value = data.getUnsignedInteger(offset, in.fieldLength)
      offset = offset + in.fieldLength
      (TemplateFields(in.informationElementID).toString, value)
    }).toMap, data, offset)

  }
}

class DefaultDataRecord(val map: Map[String, Any], val data: ByteBuf, override val length: Long) extends DataRecord {
  override def toMap: Map[String, Any] = map

}

object L2IPDataRecord {
  val LENGTH: Int = 111

  @throws[HeaderParseException]
  def apply(data: ByteBuf): Try[L2IPDataRecord] =
    Try {
      if (data.readableBytes() < 111) throw new HeaderParseException("Data array too short.")
      val sourceMacAddress = data.copy(0, 6)
      val destinationMacAddress = data.copy(6, 6)
      val ingressPhysicalInterface = data.getUnsignedInteger(12, 4)
      val egressPhysicalInterface = data.getUnsignedInteger(16, 4)
      val dot1qVlanId = data.getUnsignedInteger(20, 2).toInt
      val dot1qCustomerVlanId = data.getUnsignedInteger(22, 2).toInt
      val postDot1qVlanId = data.getUnsignedInteger(24, 2).toInt
      val postDot1qCustomerVlanId = data.getUnsignedInteger(26, 2).toInt
      val sourceIPv4Address = data.copy(28, 4).array()
      val destinationIPv4Address: Array[Byte] = data.copy(32, 4).array()
      val sourceIPv6Address = data.copy(36, 16).array()
      val destinationIPv6Address = data.copy(52, 16).array()
      val packetDeltaCount = data.getUnsignedInteger(68, 4)
      val octetDeltaCount = data.getUnsignedInteger(72, 4)
      val flowStartMilliseconds = data.getUnsignedInteger(76, 8)
      val flowEndMilliseconds = data.getUnsignedInteger(84, 8)
      val sourceTransportPort = data.getUnsignedInteger(92, 2).toInt
      val destinationTransportPort = data.getUnsignedInteger(94, 2).toInt
      val tcpOptBits = data.getInt(96)
      val protocolIdentifier = data.getInt(97)
      val ipv6ExtensionHeaders = data.getUnsignedInteger(97, 4)
      val nextHeaderIPv6 = data.getInt(102).toShort
      val flowLabelIPv6 = data.getUnsignedInteger(103, 4)
      val ipClassOfService = data.getInt(107).toShort
      val ipVersion = data.getInt(108).toShort
      val icmpTypeCodeIPv4 = data.getUnsignedInteger(109, 2).toInt
      new L2IPDataRecord(MacAddress(sourceMacAddress), MacAddress(destinationMacAddress),
        ingressPhysicalInterface, egressPhysicalInterface, dot1qVlanId,
        dot1qCustomerVlanId, postDot1qVlanId, postDot1qCustomerVlanId,
        InetAddress.getByAddress(sourceIPv4Address).asInstanceOf[Inet4Address],
        InetAddress.getByAddress(destinationIPv4Address).asInstanceOf[Inet4Address],
        InetAddress.getByAddress(sourceIPv6Address).asInstanceOf[Inet6Address],
        InetAddress.getByAddress(destinationIPv6Address).asInstanceOf[Inet6Address],
        packetDeltaCount,
        octetDeltaCount,
        flowStartMilliseconds,
        flowEndMilliseconds,
        sourceTransportPort,
        destinationTransportPort,
        tcpOptBits,
        protocolIdentifier.toShort,
        ipv6ExtensionHeaders,
        nextHeaderIPv6,
        flowLabelIPv6,
        ipClassOfService,
        ipVersion,
        icmpTypeCodeIPv4, data
      )
    }
}

class L2IPDataRecord(sourceMacAddress: MacAddress = null,
                     destinationMacAddress: MacAddress = null,
                     ingressPhysicalInterface: Long = 0L,
                     egressPhysicalInterface: Long = 0L,
                     dot1qVlanId: Int = 0,
                     dot1qCustomerVlanId: Int = 0,
                     postDot1qVlanId: Int = 0,
                     postDot1qCustomerVlanId: Int = 0,
                     sourceIPv4Address: Inet4Address = null,
                     destinationIPv4Address: Inet4Address = null,
                     sourceIPv6Address: Inet6Address = null,
                     destinationIPv6Address: Inet6Address = null,
                     packetDeltaCount: Long = 0L,
                     octetDeltaCount: Long = 0L,
                     flowStartMilliseconds: Long = 0,
                     flowEndMilliseconds: Long = 0,
                     sourceTransportPort: Int = 0,
                     destinationTransportPort: Int = 0,
                     tcpOptBits: Int = 0,
                     protocolIdentifier: Short = 0,
                     ipv6ExtensionHeaders: Long = 0L,
                     nextHeaderIPv6: Short = 0,
                     flowLabelIPv6: Long = 0L,
                     ipClassOfService: Short = 0,
                     ipVersion: Short = 0,
                     icmpTypeCodeIPv4: Int = 0,
                     override val data: ByteBuf
                    )
  extends DataRecord {


  override def toMap: Map[String, Any] =
    Map("length" -> length,
      "src_mac" -> sourceMacAddress.toString,
      "dst_mac" -> destinationMacAddress.toString,
      "ingress_physical_interface" -> ingressPhysicalInterface,
      "egress_physical_interface" -> egressPhysicalInterface,
      "dot1q_vlan_id" -> dot1qVlanId,
      "dot1q_customer_vlan_id" -> dot1qCustomerVlanId,
      "post_dot1q_vlan_id" -> postDot1qVlanId,
      "post_dot1q_customer_vlan_id" -> postDot1qCustomerVlanId,
      "src" -> sourceIPv4Address.getHostName,
      "src_ipv6" -> sourceIPv6Address.getHostName,
      //  "srcAS" -> srcAS.getOrElse(-1),
      "src_port" -> sourceIPv6Address,
      "dst_port" -> destinationTransportPort,
      //"dstAs" -> dstAS.getOrElse(-1),
      "dst" -> destinationIPv4Address.getHostName,
      "dst_ipv6" -> destinationIPv6Address.getHostName,
      "pkts" -> packetDeltaCount,
      "octetDeltaCount" -> octetDeltaCount,
      "start" -> flowStartMilliseconds,
      "stop" -> flowEndMilliseconds,
      "tcpflags" -> tcpOptBits,
      "proto" -> protocolIdentifier,
      "ipv6ExtensionHeaders" -> ipv6ExtensionHeaders,
      "nextHeaderIPv6" -> nextHeaderIPv6,
      "flowLabelIPv6" -> flowLabelIPv6,
      "ip_class_service" -> ipClassOfService,
      "ip_version" -> ipVersion,
      "icmp_type_code_ipv4" -> icmpTypeCodeIPv4

    )

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("[L2IPDataRecord]: ")
    sb.append("SourceMacAddress: ")
    sb.append(sourceMacAddress)
    sb.append(", ")
    sb.append("DestinationMacAddress: ")
    sb.append(destinationMacAddress)
    sb.append(", ")
    sb.append("IngressPhysicalInterface: ")
    sb.append(ingressPhysicalInterface)
    sb.append(", ")
    sb.append("EgressPhysicalInterface: ")
    sb.append(egressPhysicalInterface)
    sb.append(", ")
    sb.append("Dot1qVlanId: ")
    sb.append(dot1qVlanId)
    sb.append(", ")
    sb.append("Dot1qCustomerVlanId: ")
    sb.append(dot1qCustomerVlanId)
    sb.append(", ")
    sb.append("PostDot1qVlanId: ")
    sb.append(postDot1qVlanId)
    sb.append(", ")
    sb.append("PostDot1qCustomerVlanId: ")
    sb.append(postDot1qCustomerVlanId)
    sb.append(", ")
    sb.append("SourceIPv4Address: ")
    sb.append(sourceIPv4Address.getHostAddress)
    sb.append(", ")
    sb.append("DestinationIPv4Address: ")
    sb.append(destinationIPv4Address.getHostAddress)
    sb.append(", ")
    sb.append("SourceIPv6Address: ")
    sb.append(sourceIPv6Address.getHostAddress)
    sb.append(", ")
    sb.append("DestinationIPv6Address: ")
    sb.append(destinationIPv6Address.getHostAddress)
    sb.append(", ")
    sb.append("PacketDeltaCount: ")
    sb.append(packetDeltaCount)
    sb.append(", ")
    sb.append("OctetDeltaCount: ")
    sb.append(octetDeltaCount)
    sb.append(", ")
    sb.append("FlowStartMilliseconds: ")
    sb.append(flowStartMilliseconds)
    sb.append(", ")
    sb.append("FlowEndMilliseconds: ")
    sb.append(flowEndMilliseconds)
    sb.append(", ")
    sb.append("SourceTransportPort: ")
    sb.append(sourceTransportPort)
    sb.append(", ")
    sb.append("DestinationTransportPort: ")
    sb.append(destinationTransportPort)
    sb.append(", ")
    sb.append("TcpOptBits: ")
    sb.append(tcpOptBits)
    sb.append(", ")
    sb.append("ProtocolIdentifier: ")
    sb.append(protocolIdentifier)
    sb.append(", ")
    sb.append("Ipv6ExtensionHeaders: ")
    sb.append(ipv6ExtensionHeaders)
    sb.append(", ")
    sb.append("NextHeaderIPv6: ")
    sb.append(nextHeaderIPv6)
    sb.append(", ")
    sb.append("FlowLabelIPv6: ")
    sb.append(flowLabelIPv6)
    sb.append(", ")
    sb.append("IpClassOfService: ")
    sb.append(ipClassOfService)
    sb.append(", ")
    sb.append("IpVersion: ")
    sb.append(ipVersion)
    sb.append(", ")
    sb.append("IcmpTypeCodeIPv4: ")
    sb.append(icmpTypeCodeIPv4)
    return sb.toString
  }


}

object SamplingDataRecord {
  val LENGTH: Int = 16

  @throws[HeaderParseException]
  def apply(data: ByteBuf): Try[SamplingDataRecord] = Try {
    if (data.readableBytes() < LENGTH) throw new HeaderParseException("Data array too short.")
    val observationDomainId = data.getUnsignedInteger(0, 4)
    val selectorAlgorithm = data.getUnsignedInteger(4, 2).toInt
    val samplingPacketInterval = data.getUnsignedInteger(6, 4)
    val samplingPacketSpace = data.getUnsignedInteger(10, 4)
    new SamplingDataRecord(observationDomainId, selectorAlgorithm, samplingPacketInterval, samplingPacketSpace, data)
  }
}

class SamplingDataRecord(observationDomainId: Long = 0L, selectorAlgorithm: Int = 0, samplingPacketInterval: Long = 0L, samplingPacketSpace: Long = 0L
                         , override val data: ByteBuf) extends DataRecord {


  override def length: Long = {
    SamplingDataRecord.LENGTH
  }


  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("[SamplingDataRecord]: ")
    sb.append(" Observation domain ID: ")
    sb.append(observationDomainId)
    sb.append(", Selector algorithm: ")
    sb.append(selectorAlgorithm)
    sb.append(", Sampling packet interval: ")
    sb.append(samplingPacketInterval)
    sb.append(", Sampling packet space: ")
    sb.append(samplingPacketSpace)
    sb.toString
  }

  override def toMap: Map[String, Any] = Map(
    "observation_domain_id" -> observationDomainId,
    "selector_algorithm" -> selectorAlgorithm,
    "sampling_packet_interval" -> samplingPacketInterval,
    "sampling_packet_space" -> samplingPacketSpace
  )
}

object OptionTemplateRecord {
  private val HEADERLENGTH: Int = 6

  @throws[HeaderParseException]
  def apply(data: ByteBuf): Try[OptionTemplateRecord] =
    Try {
      if (data.readableBytes() < 6) throw new HeaderParseException("Data array too short.")
      val otr: OptionTemplateRecord = new OptionTemplateRecord
      val templateID = data.getUnsignedInteger(0, 2).toInt
      val fieldCount = data.getUnsignedInteger(2, 2).toInt
      val scopeFieldCount = data.getUnsignedInteger(4, 2).toInt

      val offset: Int = 6
      new OptionTemplateRecord(templateID, fieldCount, scopeFieldCount, (0 until scopeFieldCount).map(i => {


        val subData = data.copy(offset + (i * InformationElement.LENGTH), InformationElement.LENGTH)
        InformationElement(subData).toOption.orNull

      }).toList.filter(_ == null), (scopeFieldCount until scopeFieldCount).map(i => {
        val subData = data.copy(offset + (i * InformationElement.LENGTH), InformationElement.LENGTH)
        InformationElement(subData).toOption.orNull

      }).toList.filter(_ == null))
      //   otr.length = data.readableBytes()
      otr
    }

}

class OptionTemplateRecord(templateID: Int = 0, fieldCount: Int = 0, scopeFieldCount: Int = 0,
                           informationElements: List[InformationElement] = Nil,
                           scopeInformationElements: List[InformationElement] = Nil) extends TemplateRecord(templateID, fieldCount, informationElements) {
}

object InformationElement {
  val LENGTH: Int = 4

  @throws[HeaderParseException]
  def apply(data: ByteBuf): Try[InformationElement] =
    Try {
      if (data.readableBytes() < LENGTH) throw new HeaderParseException("Data array too short.")
      val informationElementID = data.getUnsignedInteger(0, 2).toInt
      val fieldLength = data.getUnsignedInteger(2, 2).toInt
      new InformationElement(informationElementID, fieldLength)
    }
}

class InformationElement(val informationElementID: Int = 0, val fieldLength: Int = 0) extends IPFIXEntity {
  val length = 4

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("[InformationElement]: ")
    sb.append("ID: ")
    sb.append(informationElementID)
    sb.append(", ")
    sb.append("Field length: ")
    sb.append(fieldLength)
    return sb.toString
  }
}

object SetHeader extends Logger {

  private val HEADERLENGTH: Int = 4

  @throws[HeaderParseException]
  def apply(data: ByteBuf, cache: TemplateProc[cflow.Template]): (SetHeader, Int) = {
    try {
      if (data.readableBytes() < 4) throw new HeaderParseException("Data array too short.")
      val setID = data.getUnsignedInteger(0, 2).toInt
      val length = data.getUnsignedInteger(2, 2).toInt


      if (setID == 2) {
        val offset: Int = 4
        val subData = data.copy(offset, length - offset)


        val tr = TemplateRecord(subData).toOption
        cache.setTemplate(tr.orNull)
        (new SetHeader(setID, templateRecords = tr.orNull :: Nil), length)

      }
      else if (setID == 3) {
        val offset: Int = 4
        val subData = data.copy(offset, length - offset)
        val otr = OptionTemplateRecord(subData).toOption
        cache.setTemplate(otr.orNull)
        (new SetHeader(setID, optionTemplateRecords = otr.orNull :: Nil), length)
      }
      else if (setID == 256) {
        val offset: Int = 4

        val subData = data.copy(offset, SamplingDataRecord.LENGTH)
        val sdr = SamplingDataRecord(subData).toOption
        (new SetHeader(setID, dataRecords = sdr.orNull :: Nil), length)


      }
      else if (setID == 306) {
        var offset: Int = 4

        var list: List[DataRecord] = Nil
        while ((length - offset - L2IPDataRecord.LENGTH) >= 0) {
          val subData = data.copy(offset, SamplingDataRecord.LENGTH)
          val lidr = L2IPDataRecord(subData).toOption
          list = lidr.orNull :: list
          offset += L2IPDataRecord.LENGTH

        }

        if ((length - offset) != 0) logger.info("Unused bytes: " + (length - offset))
        (new SetHeader(setID, dataRecords = list), length)
      }
      else if (setID > 256) {
        var offset: Int = 4
        var set_length = 0
        var list: List[DataRecord] = Nil
        while ((length - offset - set_length) >= 0) {
          val subData = data.copy(offset, data.readableBytes() - offset)
          val lidr = DefaultDataRecord(subData, cache.templates.get(setID).orNull.asInstanceOf[TemplateRecord])
          list = lidr :: list
          set_length = lidr.length.toInt
          offset = (offset + set_length).toInt

        }

        if ((length - offset) != 0) logger.info("Unused bytes: " + (length - offset))
        (new SetHeader(setID, dataRecords = list), length)
      } else {
        logger.info("Set ID " + setID + " is unknown and not handled")
        (new SetHeader(setID), length)
      }

    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        throw new HeaderParseException("Parse error: " + e.getMessage)
    }
  }
}

case class SetHeader(setID: Int = 0,
                     dataRecords: List[DataRecord] = Nil,
                     templateRecords: List[TemplateRecord] = Nil,
                     optionTemplateRecords: List[OptionTemplateRecord] = Nil) extends IPFIXEntity {

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("[SetHeader]: ")
    sb.append("Set ID: ")
    sb.append(setID)
    /* sb.append(", Length: ")
     sb.append(length)*/
    sb.append(", Data records: ")
    sb.append(dataRecords.size)
    sb.append(", ")
    for (record <- dataRecords) {
      sb.append(record)
      sb.append(", ")
    }
    sb.append("Template records: ")
    sb.append(templateRecords.size)
    sb.append(", ")
    for (record <- templateRecords) {
      sb.append(record)
      sb.append(", ")
    }
    sb.append("Option template records: ")
    sb.append(optionTemplateRecords.size)
    sb.append(", ")
    for (record <- optionTemplateRecords) {
      sb.append(record)
      sb.append(", ")
    }
    sb.toString
  }
}

object TemplateRecord {
  private val HEADERLENGTH: Int = 4

  @throws[HeaderParseException]
  def apply(data: ByteBuf): Try[TemplateRecord] = Try {
    if (data.readableBytes() < 4) throw new HeaderParseException("Data array too short.")
    val tr: TemplateRecord = new TemplateRecord
    val templateID = data.getUnsignedInteger(0, 2).toInt
    val fieldCount = data.getUnsignedInteger(2, 2).toInt
    val offset: Int = 4

    new TemplateRecord(templateID, fieldCount, (0 until fieldCount).map(i => {
      val subData = data.copy(offset + (i * InformationElement.LENGTH), InformationElement.LENGTH)
      InformationElement(subData).toOption.orNull
    }).toList)

  }
}

class TemplateRecord(val number: Int = 0, fieldCount: Int = 0, val informationElements: List[InformationElement] = Nil) extends Template {
  override def versionNumber: Int = 10


  override def map: Map[String, Int] = informationElements.map(in => {
    (TemplateFields(in.informationElementID).toString, in.fieldLength)
  }).toMap

  override def toMap: Map[String, Any] = map

  override def sender: InetSocketAddress = null

  override def id: UUID = null
}

object NetFlowIPFIXPacket extends Logger {

  private val HEADERLENGTH: Int = 16


  @throws[HeaderParseException]
  def apply(sender: InetSocketAddress, data: ByteBuf, proc: TemplateProc[cflow.Template]): NetFlowIPFIXPacket = {
    try {
      if (data.readableBytes() < 16) throw new HeaderParseException("Data array too short.")

      val version = data.getUnsignedInteger(0, 2).toInt

      val length = data.getUnsignedInteger(2, 2).toInt
      val secondsSinceEpoche: Long = data.getUnsignedInteger(4, 4)
      val milliSecondsSinceEpoche: Long = secondsSinceEpoche * 1000
      val sequenceNumber = data.getUnsignedInteger(8, 4)
      val observationDomainID = data.getUnsignedInteger(12, 4)


      var offset: Int = 16
      if (length % offset != 0) logger.info("Unused bytes: " + (length % offset))
      var subs: List[SetHeader] = Nil

      while (length - offset > 0) {
        val subData = data.copy(offset, length - offset)
        val sub = SetHeader(subData, proc)
        subs = sub._1 :: subs


        offset = offset + sub._2
      }

      new NetFlowIPFIXPacket(sender.getHostString, sender.getPort, version, new Date(milliSecondsSinceEpoche), sequenceNumber, observationDomainID, subs)
    }
    catch {
      case e: Exception => {
        throw new HeaderParseException("Parse error: " + e.getMessage)
      }
    }
  }
}

class NetFlowIPFIXPacket(
                          host: String,
                          port: Int,

                          versionNumber: Int = 0,
                          exportTime: Date = null,
                          sequenceNumber: Long = 0L,
                          observationDomainID: Long = 0L,
                          val setHeaders: List[SetHeader] = Nil) extends IPFIXEntity {

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("[MessageHeader]: ")
    sb.append("Version Number: ")
    sb.append(versionNumber)
    sb.append(", ")
    /*  sb.append("Length: ")
      sb.append(length)*/
    sb.append(", ")
    sb.append("Export time: ")
    sb.append(exportTime)
    sb.append(", ")
    sb.append("Sequence number: ")
    sb.append(sequenceNumber)
    sb.append(", ")
    sb.append("Observation Domain ID: ")
    sb.append(observationDomainID)
    sb.append(", ")
    sb.append("SetHeaders: ")
    sb.append(setHeaders.size)
    sb.append(", ")
    for (sh <- setHeaders) {
      sb.append(sh)
      sb.append(", ")
    }
    sb.toString
  }
}

@SerialVersionUID(-5309057167235377167L)
class HeaderParseException(val msg: String) extends Exception(msg) {

}

@SerialVersionUID(-2628460416419275548L)
class HeaderBytesException(val msg: String) extends Exception(msg) {
}

object MacAddress {
  def apply(address: String): MacAddress = {
    val adds = address.split(":").map(_.toInt)
    if (adds.length != 6) {
      throw new UtilityException("6 octets in a MAC address string are required.")
    }
    var i: Int = 0
    new MacAddress(adds(0), adds(1), adds(2), adds(3), adds(4), adds(5))

  }

  def apply(address: ByteBuf): MacAddress = {
    if (address.readableBytes() < 6) {
      throw new UtilityException("6 bytes are required.")
    }
    new MacAddress(address.getInt(0),
      address.getInt(1),
      address.getInt(2),
      address.getInt(3),
      address.getInt(4),
      address.getInt(5)
    )
  }

}

class MacAddress(firstOctet: Int = 0,
                 secondOctet: Int = 0,
                 thirdOctet: Int = 0,
                 fourthOctet: Int = 0,
                 fifthOctet: Int = 0,
                 sixthOctet: Int = 0) {
  override def toString: String = {
    Utility.prependZeroIfNeededForMacAddress(Integer.toHexString(firstOctet)) +
      ":" + Utility.prependZeroIfNeededForMacAddress(Integer.toHexString(secondOctet)) +
      ":" + Utility.prependZeroIfNeededForMacAddress(Integer.toHexString(thirdOctet)) +
      ":" + Utility.prependZeroIfNeededForMacAddress(Integer.toHexString(fourthOctet)) +
      ":" + Utility.prependZeroIfNeededForMacAddress(Integer.toHexString(fifthOctet)) +
      ":" + Utility.prependZeroIfNeededForMacAddress(Integer.toHexString(sixthOctet))
  }


  override def hashCode: Int = {
    (firstOctet << 40) + (secondOctet << 32) + (thirdOctet << 24) + (fourthOctet << 16) + (fifthOctet << 8) + (sixthOctet)
  }
}

/**
  * This file is part of jsFlow.
  *
  * Copyright (c) 2009 DE - CIX Management GmbH <http://www.de-cix.net>- All rights
  * reserved.
  *
  *
  *
  * This software is licensed under the Apache License, version 2.0.A copy of
  * the license agreement is included in this distribution.
  */

object Utility {
  @throws[UtilityException]
  def integerToOneByte(value: Int): Byte = {
    if ((value > Math.pow(2, 15)) || (value < 0)) {
      throw new UtilityException("Integer value " + value + " is larger than 2^15")
    }
    return (value & 0xFF).toByte
  }

  @throws[UtilityException]
  def shortToOneByte(value: Short): Byte = {
    if ((value > Math.pow(2, 15)) || (value < 0)) {
      throw new UtilityException("Integer value " + value + " is larger than 2^15")
    }
    return (value & 0xFF).toByte
  }

  @throws[UtilityException]
  def integerToTwoBytes(value: Int): Array[Byte] = {
    val result: Array[Byte] = new Array[Byte](2)
    if ((value > Math.pow(2, 31)) || (value < 0)) {
      throw new UtilityException("Integer value " + value + " is larger than 2^31")
    }
    result(0) = ((value >>> 8) & 0xFF).toByte
    result(1) = (value & 0xFF).toByte
    return result
  }

  @throws[UtilityException]
  def longToFourBytes(value: Long): Array[Byte] = {
    val result: Array[Byte] = new Array[Byte](4)
    result(0) = ((value >>> 24) & 0xFF).toByte
    result(1) = ((value >>> 16) & 0xFF).toByte
    result(2) = ((value >>> 8) & 0xFF).toByte
    result(3) = (value & 0xFF).toByte
    return result
  }

  @throws[UtilityException]
  def longToSixBytes(value: Long): Array[Byte] = {
    val result: Array[Byte] = new Array[Byte](6)
    result(0) = ((value >>> 40) & 0xFF).toByte
    result(1) = ((value >>> 32) & 0xFF).toByte
    result(2) = ((value >>> 24) & 0xFF).toByte
    result(3) = ((value >>> 16) & 0xFF).toByte
    result(4) = ((value >>> 8) & 0xFF).toByte
    result(5) = (value & 0xFF).toByte
    return result
  }

  @throws[UtilityException]
  def BigIntegerToEightBytes(value: BigInteger): Array[Byte] = {
    val result: Array[Byte] = new Array[Byte](8)
    val tmp: Array[Byte] = value.toByteArray
    if (tmp.length > 8) {
      System.arraycopy(tmp, tmp.length - 8, result, 0, 8)
    }
    else {
      System.arraycopy(tmp, 0, result, 8 - tmp.length, tmp.length)
    }
    return result
  }

  @throws[UtilityException]
  def oneByteToInteger(value: Byte): Int = {
    return value.toInt & 0xFF
  }

  @throws[UtilityException]
  def oneByteToShort(value: Byte): Short = {
    return (value & 0xFF).toShort
  }

  @throws[UtilityException]
  def twoBytesToInteger(value: Array[Byte]): Int = {
    if (value.length < 2) {
      throw new UtilityException("Byte array too short!")
    }
    val temp0: Int = value(0) & 0xFF
    val temp1: Int = value(1) & 0xFF
    ((temp0 << 8) + temp1)
  }

  @throws[UtilityException]
  def fourBytesToLong(value: Array[Byte]): Long = {
    if (value.length < 4) {
      throw new UtilityException("Byte array too short!")
    }
    val temp0: Int = value(0) & 0xFF
    val temp1: Int = value(1) & 0xFF
    val temp2: Int = value(2) & 0xFF
    val temp3: Int = value(3) & 0xFF
    ((temp0.toLong << 24) + (temp1 << 16) + (temp2 << 8) + temp3)
  }

  @throws[UtilityException]
  def sixBytesToLong(value: Array[Byte]): Long = {
    if (value.length < 6) {
      throw new UtilityException("Byte array too short!")
    }
    val temp0: Int = value(0) & 0xFF
    val temp1: Int = value(1) & 0xFF
    val temp2: Int = value(2) & 0xFF
    val temp3: Int = value(3) & 0xFF
    val temp4: Int = value(4) & 0xFF
    val temp5: Int = value(5) & 0xFF
    (((temp0.toLong) << 40) + ((temp1.toLong) << 32) + ((temp2.toLong) << 24) + (temp3 << 16) + (temp4 << 8) + temp5)
  }

  @throws[UtilityException]
  def eightBytesToBigInteger(value: Array[Byte]): BigInteger = {
    if (value.length < 8) {
      throw new UtilityException("Byte array too short!")
    }
    val bInt: BigInteger = new BigInteger(1, value)
    bInt
  }

  def dumpBytes(data: Array[Byte]): String = {
    val sb: StringBuilder = new StringBuilder
    var i: Int = 0
    for (b <- data) {
      i += 1
      sb.append(String.valueOf(b))
      if (i < data.length) sb.append(", ")
      if ((i % 15) == 0) sb.append("\n")
    }
    return sb.toString
  }

  def prependZeroIfNeededForMacAddress(string: String): String = {
    if (string == null) return "00"
    if (string.length == 0) return "00"
    if (string.length == 1) return "0" + string
    return string
  }

  def isConfigured(inet4Address: Inet4Address): Boolean = {
    if (inet4Address == null) return false
    try {
      if (inet4Address == InetAddress.getByName("0.0.0.0")) return false
    }
    catch {
      case e: UnknownHostException => {
        return false
      }
    }
    true
  }

  def isConfigured(inet6Address: Inet6Address): Boolean = {
    if (inet6Address == null) return false
    try {

      if (inet6Address == InetAddress.getByName("0:0:0:0:0:0:0:0")) return false
    }
    catch {
      case e: UnknownHostException => {
        return false
      }
    }
    true
  }
}

@SerialVersionUID(3545800974716581680L)
class UtilityException(val mesg: String) extends Exception(mesg) {
}