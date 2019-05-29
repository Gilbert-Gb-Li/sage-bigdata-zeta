package com.haima.sage.bigdata.etl.reader

import java.util.concurrent.TimeUnit

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{ConnectionFactory, Consumer, Envelope, ShutdownSignalException}
import org.junit.Test

import scala.io.Source

/**
  * Created by zhhuiyan on 2016/10/17.
  */
class RabbitMQTest {
  val factory = new ConnectionFactory()
  factory.setHost("127.0.0.1")
  factory.setPort(5672)
  factory.setVirtualHost("/")
  factory.setUsername("raysdata")
  factory.setPassword("raysdata")

  val endPoint = "endpoint"
  val (connection, channel) = getChannel(endPoint)


  @Test
  def PingPang(): Unit = {
    product(Source.fromFile("data/cmb.pro/4.json").getLines().mkString(""))
    // consume()
    /*(0 to 1000).foreach(i=>product(s"ping:$i"))
    (0 to 1000).foreach(i=>product(s"pang:$i"))*/
    /* product("ping")
     product("pang")*/
    TimeUnit.SECONDS.sleep(1)
    close()
  }


  def getChannel(endpoint: String) = {
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    //declaring a queue for this channel. If queue does not exist,
    //it will be created on the server.
    channel.queueDeclare(endPoint, false, false, false, null)
    (connection, channel)
  }

  @Test
  def consume(): Unit = {
    try {
      var count = 0
      //start consuming messages. Auto acknowledge messages.
      channel.basicConsume(endPoint, true, new Consumer {
        override def handleCancel(s: String) = {}

        override def handleRecoverOk(s: String) = {

        }

        override def handleCancelOk(s: String) = {

        }

        override def handleDelivery(s: String,
                                    envelope: Envelope,
                                    basicProperties: BasicProperties,
                                    body: Array[Byte]) = {
          count += 1
          System.out.println(s"[$count] Message: " + new String(body) + " received.")
        }

        override def handleShutdownSignal(s: String, e: ShutdownSignalException) = {

        }

        override def handleConsumeOk(consumerTag: String) = {

          println("Consumer " + consumerTag + " registered")
        }
      })
    } catch {
      case e: Exception =>

        e.printStackTrace();
    }

    TimeUnit.HOURS.sleep(10)
  }

  def product(obj: String): Unit = {
    channel.basicPublish("", endPoint, null, obj.getBytes)
  }

  /**
    * 关闭channel和connection。并非必须，因为隐含是自动调用的。
    */
  def close() {
    this.channel.close()
    this.connection.close()
  }

}
