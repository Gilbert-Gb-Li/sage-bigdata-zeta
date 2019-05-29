package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.NetSource
import com.haima.sage.bigdata.etl.stream.SnmpWalkStream
import org.snmp4j._
import org.snmp4j.mp.{MPv1, MPv2c, MPv3, SnmpConstants}
import org.snmp4j.security._
import org.snmp4j.smi._
import org.snmp4j.transport.{DefaultTcpTransportMapping, DefaultUdpTransportMapping}
import org.snmp4j.util.{MultiThreadedMessageDispatcher, ThreadPool}

class SnmpWalkLogReader(conf: NetSource) extends LogReader[RichMap] {
  val protocol = conf.protocol.getOrElse("udp")
  val host = conf.host.getOrElse("0.0.0.0")
  val port = conf.port
  val targetOid = conf.get("targetOid", "")
  val community = conf.get("community", "public")

  val version: String = conf.get("version", "v2")
  val timeout: Long = conf.timeout
  val retry: Int = 1


  private[SnmpWalkLogReader] val pathName: String = s"$protocol:$host/$port"
  val address: Address = GenericAddress.parse(pathName)
  val target: CommunityTarget = new CommunityTarget
  target.setCommunity(new OctetString(community))
  target.setAddress(address)
  version match {
    case v: String if v.equals("v1") =>
      target.setVersion(SnmpConstants.version1)
    case v: String if v.equals("v3") =>
      target.setVersion(SnmpConstants.version3)
    case _ =>
      target.setVersion(SnmpConstants.version2c)
  }
  target.setTimeout(timeout) // milliseconds
  target.setRetries(retry)


  val transport: DefaultUdpTransportMapping = new DefaultUdpTransportMapping
  val threadPool: ThreadPool = ThreadPool.create("snmptrap-pool", 3)
  val dispatcher: MessageDispatcher = new MultiThreadedMessageDispatcher(threadPool, new MessageDispatcherImpl)
  val snmp: Snmp = new Snmp(dispatcher, transport)
  version match {
    case v: String if v.equals("v1") =>
      snmp.getMessageDispatcher.addMessageProcessingModel(new MPv1)
    case v: String if v.equals("v3") =>
      val usm: USM = new USM(SecurityProtocols.getInstance.addDefaultProtocols(), MPv3.createLocalEngineID, 0)
      snmp.getMessageDispatcher.addMessageProcessingModel(new MPv3(usm))
      SecurityModels.getInstance.addSecurityModel(usm)
      usm.addUser(conf.get("user", ""), new UsmUser(conf.get("user", ""), conf.get("auth_protocol", ""), conf.get("auth_key", ""), conf.get("priv_protocol", ""), conf.get("priv_key", "")))
    case _ =>
      snmp.getMessageDispatcher.addMessageProcessingModel(new MPv2c)
  }

  snmp.listen()
  val stream = SnmpWalkStream(snmp, target, targetOid, conf.timeout) //



  implicit def octet(key: String): OctetString = {
    new OctetString(key)
  }

  implicit def octet(key: Array[Byte]): OctetString = {
    new OctetString(key)
  }

  implicit def oid(key: String): OID = {
    key match {
      case "sha" =>
        AuthSHA.ID
      case "md5" =>
        AuthMD5.ID
      case "des" =>
        PrivDES.ID
      case "aes128" =>
        PrivAES128.ID
      case "aes192" =>
        PrivAES192.ID
      case "aes256" =>
        PrivAES256.ID
    }
  }

  def skip(skip: Long): Long = 0

  override def path: String = pathName

}