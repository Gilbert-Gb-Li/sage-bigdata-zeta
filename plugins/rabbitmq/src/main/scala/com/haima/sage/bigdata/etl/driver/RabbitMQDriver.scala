package com.haima.sage.bigdata.etl.driver

import com.rabbitmq.client.ConnectionFactory
import com.haima.sage.bigdata.etl.utils.Logger

import scala.util.Try

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class RabbitMQDriver(mate: RabbitMQMate) extends Driver[ConnectionFactory] with Logger {


  def driver() = Try {
    val factory = new ConnectionFactory()
    factory.setHost(mate.host)
    //factory.setAutomaticRecoveryEnabled(true)

    mate.port match {
      case Some(port: Int) =>
        factory.setPort(port)
      case _ =>
    }
    mate.virtualHost match {
      case Some(vHost: String) if vHost.trim != "" =>
        factory.setVirtualHost(vHost)
      case _ =>
    }
    mate.username match {
      case Some(uname: String) if uname.trim != "" =>
        factory.setUsername(uname)
      case _ =>
    }

    mate.password match {
      case Some(pword: String) if pword.trim != "" =>
        factory.setPassword(pword)
      case _ =>
    }
    factory
  }
}
