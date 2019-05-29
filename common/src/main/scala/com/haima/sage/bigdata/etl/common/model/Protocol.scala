package com.haima.sage.bigdata.etl.common.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[TCP], name = "tcp"),
  new Type(value = classOf[UDP], name = "udp"),
  new Type(value = classOf[Net], name = "net"),
  new Type(value = classOf[Akka], name = "akka"),
  new Type(value = classOf[Syslog], name = "syslog"),
  new Type(value = classOf[NetFlow], name = "netflow"),
  new Type(value = classOf[JFlow], name = "jflow"),
  new Type(value = classOf[SnmpWalk], name = "snmpwalk"),
  new Type(value = classOf[SnmpTrap], name = "snmptrap")
))
trait Protocol {
  override def toString: String = this.getClass.getSimpleName.toLowerCase
}

case class TCP() extends Protocol

case class UDP() extends Protocol

@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[Net], name = "net"),
  new Type(value = classOf[Akka], name = "akka"),
  new Type(value = classOf[Syslog], name = "syslog"),
  new Type(value = classOf[NetFlow], name = "netflow"),
  new Type(value = classOf[JFlow], name = "jflow"),
  new Type(value = classOf[SnmpWalk], name = "snmpwalk"),
  new Type(value = classOf[SnmpTrap], name = "snmptrap")
))
trait WithProtocol extends Protocol {
  def protocol: Protocol
}


case class Net(protocol: Protocol = UDP()) extends WithProtocol {

}

case class Akka() extends WithProtocol {
  val protocol = TCP()

}


case class Syslog(protocol: Protocol = UDP()) extends WithProtocol {

}

case class NetFlow() extends WithProtocol {
  val protocol: Protocol = UDP()
}

case class JFlow() extends WithProtocol {
  val protocol: Protocol = UDP()
}

case class SnmpWalk(protocol: Protocol = UDP()) extends WithProtocol {

}

case class SnmpTrap(protocol: Protocol = UDP()) extends WithProtocol {

}



