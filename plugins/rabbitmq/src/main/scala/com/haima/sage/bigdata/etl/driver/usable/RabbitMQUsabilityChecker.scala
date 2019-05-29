package com.haima.sage.bigdata.etl.driver.usable

import com.rabbitmq.client.{Channel, Connection}
import com.haima.sage.bigdata.etl.common.model.{Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.{RabbitMQDriver, RabbitMQMate}

import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class RabbitMQUsabilityChecker(mate: RabbitMQMate) extends UsabilityChecker {
  val driver = RabbitMQDriver(mate)
  val msg: String = mate.uri + "error:"

  override def check: Usability = {
    driver.driver() match {
      case Success(factory) =>
        var connection: Connection = null
        var channel: Channel = null
        try {
          connection = factory.newConnection()
          channel = connection.createChannel()
          Usability()
        } catch {
          case e: Exception =>
            Usability(usable = false, cause = msg + e.getMessage)
        } finally {
          if (channel != null) {
            channel.close()
          }
          if (connection != null) {
            connection.close()
          }
        }
      case Failure(e) =>
        Usability(usable = false, cause = msg + e.getMessage)
    }


  }
}
