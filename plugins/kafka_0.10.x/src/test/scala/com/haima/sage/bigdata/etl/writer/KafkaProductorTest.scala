package com.haima.sage.bigdata.etl.writer

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model.JsonParser
import com.haima.sage.bigdata.etl.lexer.JSONLogLexer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.junit.Test

/**
  * Created by zhhuiyan on 16/3/3.
  */
class KafkaProductorTest {

  import scala.collection.JavaConversions._

  var format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  var cache: Int = 0
  /* props.put(Contents.ZOOKEEPER_CONNECT, "127.0.0.1:2181")*/
  val props: Map[String, AnyRef] = Map("producer.type" -> "async", "metadata.broker.list" -> "127.0.0.1:9092")


  val producer = new KafkaProducer[String, Array[Byte]](props)

  @Test
  def sendTest(): Unit = {


    println(JSONLogLexer.apply(JsonParser()).parse(s"""{"index":1000,"name":"kafka-test","data":"${UUID.randomUUID().toString}"}"""))
    var i = 0l
    while (i < 1) {
      i += 1

      val date = format.format(new Date())
      val data = s"""$date@{"index":$i,"name":"kafka-test-zkA","data":"${UUID.randomUUID().toString}"}"""
      producer.send(new ProducerRecord[String, Array[Byte]]("topic1", data.getBytes))
      cache += 1 //producer.send(new KeyedMessage[String, Array[Byte]]("kafka-test", (date + "@index:" + i).getBytes))
    }
    TimeUnit.SECONDS.sleep(2)
    producer.close()
  }

}
