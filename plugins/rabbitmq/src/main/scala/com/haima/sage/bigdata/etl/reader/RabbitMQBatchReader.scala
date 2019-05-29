package com.haima.sage.bigdata.etl.reader

import java.io.IOException
import java.util.concurrent.{TimeUnit, TimeoutException}

import akka.actor.ActorSelection
import com.rabbitmq.client._
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{Event, RabbitMQSource, ReadPosition, Stream}
import com.haima.sage.bigdata.etl.driver.RabbitMQDriver

import scala.util.{Failure, Success}

class RabbitMQBatchReader(conf: RabbitMQSource) extends LogReader[Event] with Position {
  val position = ReadPosition(conf.uri, 0, 0)
  private final val CACHE_SIZE: Long = CONF.getLong(PROCESS_CACHE_SIZE) match {
    case 0 =>
      PROCESS_CACHE_SIZE_DEFAULT
    case a =>
      a
  }
  val factory: ConnectionFactory = RabbitMQDriver(conf).driver() match {
    case Success(_factory) =>
      _factory
    case Failure(e) =>
      throw e;
  }

  val connection: Connection = factory.newConnection()
  val channel: Channel = connection.createChannel()
  // channel.queueDeclare(conf.queue, conf.durable, false, false, null)
  channel.basicQos(0, (CACHE_SIZE / 2).toInt, false)

  override val stream = new Stream[Event](None) {


    private var item: Event = _

    final def hasNext: Boolean = {
      state match {
        case State.done | State.fail =>
          false

        case State.init =>
          var response = channel.basicGet(conf.queue, false)

          if (response == null) {
            TimeUnit.MILLISECONDS.sleep(conf.timeout)
            response = channel.basicGet(conf.queue, false)
            if (response == null) {
              throw new TimeoutException(s"waiting ${conf.timeout} ms ,no data received !")
            }
          }
          item = Event(None, conf.encoding match {
            case Some(enc: String) =>
              new String(response.getBody, enc)
            case _ =>
              new String(response.getBody)
          })
          position.recordIncrement()
          position.position = response.getEnvelope.getDeliveryTag


          //  channel.basicAck(position.position, false)
          ready()
          true
        case State.ready =>
          true
      }
    }


    override def next(): Event = {
      init()

      item
    }
  }


  def skip(skip: Long): Long = 0

  override def path: String = conf.uri

  @throws(classOf[IOException])
  override def close() {
    try {
      if (this.channel.isOpen)
        this.channel.close()
      if (this.connection.isOpen)
        this.connection.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()

    }

    super.close()
  }

  override def callback(batch: ReadPosition)(implicit positionServer: ActorSelection): Unit = {
    channel.basicAck(batch.position, true)
    super.callback(batch)(positionServer)
  }
}