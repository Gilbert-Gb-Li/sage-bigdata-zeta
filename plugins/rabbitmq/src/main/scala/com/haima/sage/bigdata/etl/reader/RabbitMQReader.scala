package com.haima.sage.bigdata.etl.reader

import java.io.IOException

import com.rabbitmq.client.ConnectionFactory
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{Event, RabbitMQSource}
import com.haima.sage.bigdata.etl.stream.RabbitMQStream

class RabbitMQReader(conf: RabbitMQSource) extends LogReader[Event] {

  logger.debug(s"Rabbit:" + conf)

  val factory = new ConnectionFactory()
  factory.setHost(conf.host)
  //factory.setAutomaticRecoveryEnabled(true)

  conf.port match {
    case Some(port: Int) =>
      factory.setPort(port)
    case _ =>
  }
  conf.virtualHost match {
    case Some(vHost: String) if vHost.trim != "" =>
      factory.setVirtualHost(vHost)
    case _ =>
  }
  conf.username match {
    case Some(uname: String) if uname.trim != "" =>
      factory.setUsername(uname)
    case _ =>
  }

  conf.password match {
    case Some(pword: String) if pword.trim != "" =>
      factory.setPassword(pword)
    case _ =>
  }

  override val stream = new RabbitMQStream(factory, conf.queue, conf.durable, conf.encoding, conf.timeout)

  def skip(skip: Long): Long = 0

  override def path: String = conf.uri

  @throws(classOf[IOException])
  override def close() {
    super.close()
  }
}