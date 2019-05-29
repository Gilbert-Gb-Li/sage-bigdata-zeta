package com.haima.sage.bigdata.etl.reader

import org.junit.{After, Test}
import org.snmp4j._
import org.snmp4j.mp.{MPv3, SnmpConstants}
import org.snmp4j.security._
import org.snmp4j.smi._
import org.snmp4j.transport.DefaultUdpTransportMapping
import org.snmp4j.util.DefaultPDUFactory

/**
 * https://github.com/mchopker/myprojects/tree/master/TranSenderReceiver/src
 *
 * @author taoistwar
 *
 */
class SnmpTrapSender {
  private val community: String = "public"
  private val trapOid: String = "1.1.3.6.1.2.1.1.6"
  private val ipAddress: String = "127.0.0.1"
  private val port: Int = 5000
  val snmp: Snmp = new Snmp(new DefaultUdpTransportMapping)
  snmp.listen()




  @After
  def clearUp(): Unit = {
    snmp.close()

  }

  @Test
  def sendV1Trap(): Unit = {
    (1 to 100000).foreach {
      i =>
        sendSnmpV1V2Trap(SnmpConstants.version1)
    }

  }

  @Test
  def sendV2Trap(): Unit = {
    (1 to 1000).foreach {
      i =>
        sendSnmpV1V2Trap(SnmpConstants.version2c)
    }


  }

  @Test
  def sendV3Trap(): Unit = {
    (1 to 1000).foreach {
      i =>
        sendSnmpV3Trap()
    }
  }

  /**
   * This methods sends the V1/V2 trap
   *
   * @param version
   */
  private def sendSnmpV1V2Trap(version: Int) {
    sendV1orV2Trap(version, community, ipAddress, port)
  }

  private def createPdu(snmpVersion: Int): PDU = {
    val pdu: PDU = DefaultPDUFactory.createPDU(snmpVersion)
    pdu.setType(if (snmpVersion == SnmpConstants.version1) PDU.V1TRAP else PDU.TRAP)
    pdu.add(new VariableBinding(SnmpConstants.sysUpTime))
    pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, new OID(trapOid)))
    pdu.add(new VariableBinding(SnmpConstants.snmpTrapAddress, new IpAddress(ipAddress)))
    pdu.add(new VariableBinding(new OID(trapOid), new OctetString("Major")))
    pdu
  }

  private def sendV1orV2Trap(snmpVersion: Int, community: String, ipAddress: String, port: Int) {
    try {
      val communityTarget: CommunityTarget = new CommunityTarget
      communityTarget.setCommunity(new OctetString(community))
      communityTarget.setVersion(snmpVersion)
      communityTarget.setAddress(new UdpAddress(ipAddress + "/" + port))
      communityTarget.setRetries(2)
      communityTarget.setTimeout(5000)

      snmp.send(createPdu(snmpVersion), communityTarget)
      System.out.println("Sent Trap to (IP:Port)=> " + ipAddress + ":" + port)
    }
    catch {
      case e: Exception =>
        System.err.println("Error in Sending Trap to (IP:Port)=> " + ipAddress + ":" + port)
        System.err.println("Exception Message = " + e.getMessage)
    }
  }

  /**
   * Sends the v3 trap
   */
  private def sendSnmpV3Trap() {
    try {
      val targetAddress: Address = GenericAddress.parse("udp:" + ipAddress + "/" + port)
      val usm: USM = new USM(SecurityProtocols.getInstance.addDefaultProtocols(),
        new OctetString(MPv3.createLocalEngineID), 0)
      SecurityModels.getInstance.addSecurityModel(usm)
      snmp.getUSM.addUser(new OctetString("taoist"),
        new UsmUser(new OctetString("taoist"), AuthMD5.ID,
          new OctetString("12345678"),
          PrivAES128.ID,
          new OctetString("12345678")))
      val target: UserTarget = new UserTarget
      target.setAddress(targetAddress)
      target.setRetries(1)
      target.setTimeout(11500)
      target.setVersion(SnmpConstants.version3)
      target.setSecurityLevel(SecurityLevel.AUTH_PRIV)
      target.setSecurityName(new OctetString("taoist"))
      val pdu: PDU = new ScopedPDU
      pdu.setType(PDU.NOTIFICATION)
      pdu.add(new VariableBinding(SnmpConstants.sysUpTime))
      pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, SnmpConstants.linkDown))
      pdu.add(new VariableBinding(new OID(trapOid), new OctetString("Major")))
      snmp.send(pdu, target)
      System.out.println("Sending Trap to (IP:Port)=> " + ipAddress + ":" + port)
      snmp.addCommandResponder(new CommandResponder {
        def processPdu(arg0: CommandResponderEvent) {
          System.out.println(arg0)
        }
      })
    }
    catch {
      case e: Exception =>
        System.err.println("Error in Sending Trap to (IP:Port)=> " + ipAddress + ":" + port)
        System.err.println("Exception Message = " + e.getMessage)
    }
  }

}
