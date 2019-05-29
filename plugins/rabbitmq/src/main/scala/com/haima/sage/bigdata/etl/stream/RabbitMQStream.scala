package com.haima.sage.bigdata.etl.stream

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._
import com.haima.sage.bigdata.etl.common.model.Event
import com.haima.sage.bigdata.etl.utils.Logger

/**
  * Created by zhhuiyan on 16/5/10.
  */
class RabbitMQStream(factory: ConnectionFactory, queueName: String, durable: Boolean, encoding: Option[String], _timeout: Long) extends QueueStream[Event](None, _timeout) with Logger {

  val (connection, channel) = getChannel

  def getChannel(): (Connection, Channel) = {
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    //declaring a queue for this channel. If queue does not exist,
    //it will be created on the server.
    channel.queueDeclare(queueName, durable, false, false, null)
    channel.basicConsume(queueName, false, new RabbitConsumerHandler(channel))
    channel.basicQos(0, (CACHE_SIZE / 2).toInt, false)
    (connection, channel)
  }


  override def close(): Unit = {
    super.close()
    this.channel.close()
    this.connection.close()

  }

  class RabbitConsumerHandler(channel: Channel) extends Consumer with Logger {


    override def handleCancel(s: String): Unit = {
      logger.debug(s" handle cancel")
    }

    override def handleRecoverOk(s: String): Unit = {
      logger.debug(s" handle recover ok")

    }

    override def handleCancelOk(s: String): Unit = {
      logger.debug(s" handle cancel ok")

    }

    override def handleDelivery(s: String,
                                envelope: Envelope,
                                basicProperties: BasicProperties,
                                body: Array[Byte]): Unit = {
      val event = Event(None, encoding match {
        case Some(enc: String) =>
          new String(body, enc)
        case _ =>
          new String(body)
      })
      if (queue.size() >= CACHE_SIZE) {
        channel.basicReject(envelope.getDeliveryTag, true)
      } else {
        logger.debug(s" received $event")
        channel.basicAck(envelope.getDeliveryTag, false)
        queue.add(event)
      }

    }

    override def handleShutdownSignal(s: String, e: ShutdownSignalException): Unit = {
      logger.debug(s" shut down")
    }

    override def handleConsumeOk(consumerTag: String): Unit = {
      logger.debug(s"consumer $consumerTag registered")
    }
  }

}
