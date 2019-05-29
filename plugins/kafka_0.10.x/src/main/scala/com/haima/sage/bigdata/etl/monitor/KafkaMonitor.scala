package com.haima.sage.bigdata.etl.monitor

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.driver.KafkaDriver
import com.haima.sage.bigdata.etl.reader.KafkaAllLogReader
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.util.{Failure, Success}


class KafkaMonitor(conf: KafkaSource, parser: Parser[MapRule]) extends Monitor {
  implicit var id: String = _
  val encoding: Option[String] = conf.get("encoding")

      @throws[Exception]
  override def run(): Unit = {

    KafkaDriver(conf).driver() match {
      case Success(config) =>
        try {
          val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)

          send(Some(new KafkaAllLogReader(consumer,
            conf.uri, conf.topic.getOrElse("topic"), conf.wildcard, conf.position,
            WAIT_TIME.toInt, conf.codec, encoding, Some(positionServer),id)), Some(parser))
        } catch {
          case e: Exception =>
            e.printStackTrace()
            logger.error(s"create reader error:$e")
            context.parent ! (ProcessModel.MONITOR, Status.ERROR,s"MONITOR_ERROR:${e.getMessage}")
            this.close()
            context.stop(self)
        }
      case Failure(e) =>
        logger.error(s"create reader error:$e")
    }


  }

  override def close(): Unit = {

  }
  override def receive: Receive = {
    //获取数据通道的ID,用于position,sender() 是process
    case id: String =>
      this.id = id
    case obj =>
      super.receive(obj)
  }
}
