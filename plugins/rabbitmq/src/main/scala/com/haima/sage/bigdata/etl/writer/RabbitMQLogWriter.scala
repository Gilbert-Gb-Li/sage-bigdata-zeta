package com.haima.sage.bigdata.etl.writer

import java.io.IOException

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.exception.LogWriteException
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.writer.NameFormatter
import com.haima.sage.bigdata.etl.metrics.MeterReport


import scala.concurrent.duration._

@SuppressWarnings(Array("serial"))
class RabbitMQLogWriter(conf: RabbitMQWriter, report: MeterReport) extends DefaultWriter[RabbitMQWriter](conf, report: MeterReport) with BatchProcess {
  import  context.dispatcher
  private final val mapper = Formatter(conf.contentType)

  private final val formatter = NameFormatter(conf.queue, conf.persisRef)
  private var existQueues: Set[String] = Set()

  val factory = new ConnectionFactory()
  factory.setHost(conf.host)
  //factory.setAutomaticRecoveryEnabled(true)

  conf.port match {
    case Some(port: Int) =>
      factory.setPort(port)
    case _ =>
  }
  conf.virtualHost match {
    case Some(vHost: String) =>
      factory.setVirtualHost(vHost)
    case _ =>
  }
  conf.username match {
    case Some(uname: String) =>
      factory.setUsername(uname)
    case _ =>
  }

  conf.password match {
    case Some(pword: String) =>
      factory.setPassword(pword)
    case _ =>
  }

  def init(): (Connection, Channel) = {
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    //declaring a queue for this channel. If queue does not exist,
    //it will be created on the server.

    (connection, channel)
  }

  val (connection, channel) = init()

  private var count = 0


  final def flush() {
    report()
  }

  private var item: Map[String, Any] = _

  @throws(classOf[LogWriteException])
  def write(t: RichMap) {
    val queueName = formatter.format(t)
    try {

      if (!existQueues.contains(queueName)) {
        existQueues += queueName
        channel.queueDeclare(queueName, conf.durable, false, false, null)
      }
      val f = mapper.bytes(t)
      channel.basicPublish("", queueName, null, f)


      /*try {

        logger.debug(s"rabbitMQ publishedstart:${cached}")
      logger.debug(s"rabbitMQ published data :${f.length}")
        channel.basicPublish("", queueName, null, f)

        logger.debug(s"rabbitMQ published end:${cached}")
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }*/

      if (!connect) {
        connect = connect
        count = 0
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.RUNNING)

      }

      if (getCached % cacheSize == 0) {
        logger.debug(s"rabbitMQ received:${getCached}")
        this.flush()
        item = null
      }

    } catch {
      case e: Exception =>
        logger.error(s"write to rabbitmq error $e ")
        if (count <= 30) {
          context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR)
          count = count + 1
          item = t
          context.system.scheduler.scheduleOnce(1 seconds, self, ("try", item))
        }
    }

  }


  @throws(classOf[IOException])
  def close() {
    if (connect || item == null) {
      this.flush()
      context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
      channel.close()
      connection.close()
      logger.debug(s"rabbitmq closed")
      context.stop(self)
    } else {
      self ! item
      self ! (self.path.toString, STOP)
      item = null
    }


  }


  override def redo(): Unit = {

  }

  override def receive: Receive = {
    case ("try", data: RichMap) =>
      write(data)
    case any =>
      super.receive(any)
  }
}