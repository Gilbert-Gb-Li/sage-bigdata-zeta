package com.haima.sage.bigdata.etl.common.model

import java.util.{Date, UUID}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.haima.sage.bigdata.etl.common.model.Status.Status


/**
  * Created by zhhuiyan on 15/4/9.
  */
@JsonIgnoreProperties(Array("createtime", "lasttime"))
case class Collector(id: String = UUID.randomUUID().toString,
                     host: String,
                     port: Int,
                     system: String = "worker",
                     name: String = "",
                     relayServer: Option[Collector] = None,
                     @JsonScalaEnumeration(classOf[StatusType])
                     status: Option[Status] = None,
                     lasttime: Option[Date] = Some(new Date()),
                     createtime: Option[Date] = Some(new Date())) extends LastAndCreateTime {


  def address(): String = {
    s"akka.tcp://$system@$host:$port"
  }


}

object Collectors {
  private final val reg = "akka.tcp://(.*)@(.*):(\\d*)/?.*".r

  def from(address: String): Option[Collector] = {
    reg.unapplySeq(address) match {
      case Some(system :: host :: port :: Nil) =>

        Some(Collector(host = host, port = port.toInt, system = system))
      case _ => None
    }
  }

}

