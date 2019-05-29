package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.NetSource
import com.haima.sage.bigdata.etl.stream.SnmpTrapStream
import org.snmp4j._
import org.snmp4j.mp.{MPv1, MPv2c, MPv3}
import org.snmp4j.security._
import org.snmp4j.smi._
import org.snmp4j.transport.{DefaultTcpTransportMapping, DefaultUdpTransportMapping}
import org.snmp4j.util.{MultiThreadedMessageDispatcher, ThreadPool}

class SnmpTrapLogReader(conf: NetSource) extends LogReader[RichMap] {
  val protocol = conf.protocol.getOrElse("udp")
  val host = conf.host.getOrElse("0.0.0.0")
  val port = conf.port

  val version = conf.get("version", "")

  private[SnmpTrapLogReader] val pathName: String = s"$protocol://$host:$port"

  val threadPool: ThreadPool = ThreadPool.create("snmptrap-pool", 3)
  val dispatcher: MessageDispatcher = new MultiThreadedMessageDispatcher(threadPool, new MessageDispatcherImpl)
  val snmp: Snmp = new Snmp(dispatcher, transport())
  snmp.getMessageDispatcher.addMessageProcessingModel(new MPv1)
  snmp.getMessageDispatcher.addMessageProcessingModel(new MPv2c)
  val usm: USM = new USM(SecurityProtocols.getInstance.addDefaultProtocols(), MPv3.createLocalEngineID, 0)

  snmp.getMessageDispatcher.addMessageProcessingModel(new MPv3(usm))
  SecurityModels.getInstance.addSecurityModel(usm)

  /* val smiManager = new SmiManager("8b f5 3e 80 29 1f 4a 47 / atAwIOFz", new File("myEmptyDirectory"))
   SNMP4JSettings.setOIDTextFormat(smiManager)
   SNMP4JSettings.setVariableTextFormat(smiManager)*/


  if ("v3" == version) {
    usm.addUser(conf.get("user", ""), new UsmUser(conf.get("user", ""), conf.get("auth_protocol", ""), conf.get("auth_key", ""), conf.get("priv_protocol", ""), conf.get("priv_key", "")))
  }
  snmp.listen()
  val stream = SnmpTrapStream(snmp, conf.timeout) //

  private[SnmpTrapLogReader] def transport(): TransportMapping[_ <: Address] = {
    protocol match {
      case "tcp" =>
        new DefaultTcpTransportMapping(new TcpAddress(host + "/" + port))
      case _ =>
        new DefaultUdpTransportMapping(new UdpAddress(host + "/" + port))
    }
  }

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