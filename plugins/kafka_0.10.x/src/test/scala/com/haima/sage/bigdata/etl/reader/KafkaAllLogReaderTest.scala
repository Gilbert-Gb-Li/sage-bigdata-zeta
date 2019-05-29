package com.haima.sage.bigdata.etl.reader

import java.util
import java.util.Properties
import java.util.concurrent.TimeoutException

import com.haima.sage.bigdata.etl.common.model.{KafkaSource, ProcessFrom}
import com.haima.sage.bigdata.etl.driver.KafkaDriver
import com.haima.sage.bigdata.etl.utils.{Logger, WildMatch}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, Partitioner, ProducerRecord}
import org.apache.kafka.common.Cluster
import org.junit.Test

/**
  * 测试之前先运行producer创建测试数据，
  * sage-test-out是只有一个分区的topic,
  * sage-test是具有三个分区的topic,测试前请自行创建。
  */
@Test
class KafkaAllLogReaderTest extends WildMatch {

//  @Test
//  def flinkConsumer(): Unit = {
//    val consumer = new FlinkKafkaConsumer011[String]("topic",
//      new KeyedDeserializationSchemaWrapper[String](new SimpleStringSchema()), new Properties())
//
//  }


  val hostPorts = "10.10.106.69:9092,10.10.106.70:9092,10.10.106.89:9092,10.10.106.90:9092"
  //val hostPorts="10.10.106.169:9092,10.10.106.170:9092,10.10.109.89:9092,10.10.106.190:9092"
  val topics = Array("sage-test-out", "sage-test,sage-test-out", "sage-test*", "sage-test:1,2")

  //验证只有一个topic
  @Test
  def testConsumer(): Unit = {
    val kafkaSourceConf = KafkaSource(hostPorts, Some(topics(0)), None, "false", Some(ProcessFrom.START), None, None)
    val config = KafkaDriver(kafkaSourceConf).driver().get
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)
    val kafkaAllLogReader: KafkaAllLogReader = new KafkaAllLogReader(consumer, kafkaSourceConf.uri, kafkaSourceConf.topic.getOrElse("topic"),
      kafkaSourceConf.wildcard, kafkaSourceConf.position, 500,
      kafkaSourceConf.codec, None, None, null)
    val stream = kafkaAllLogReader.stream
    var count = 0
    try {
      while (stream.hasNext) {
        stream.next()
        count = count + 1
      }
    } catch {
      case _: TimeoutException =>
        stream.close()
        kafkaAllLogReader.close()
        assert(count == 50)
      case _: Exception =>
        assert(false)
    }
  }

  //验证topic正则情况
  @Test
  def testConsumerReg(): Unit = {
    val kafkaSourceConf = KafkaSource(hostPorts, Some(topics(2)), None, "true", Some(ProcessFrom.START), None, None)
    val config = KafkaDriver(kafkaSourceConf).driver().get
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)
    val kafkaAllLogReader: KafkaAllLogReader = new KafkaAllLogReader(consumer, kafkaSourceConf.uri, kafkaSourceConf.topic.getOrElse("topic"),
      kafkaSourceConf.wildcard, kafkaSourceConf.position, 500,
      kafkaSourceConf.codec, None, None, null)
    val stream = kafkaAllLogReader.stream
    var count = 0
    try {
      while (stream.hasNext) {
        stream.next()
        count = count + 1
      }
    } catch {
      case _: TimeoutException =>
        stream.close()
        kafkaAllLogReader.close()
        assert(count == 150)
      case _: Exception =>
        assert(false)
    }
  }

  //验证指定多个topic的情况
  @Test
  def testConsumerArr(): Unit = {
    val kafkaSourceConf = KafkaSource(hostPorts, Some(topics(1)), None, "false", Some(ProcessFrom.START), None, None)
    val config = KafkaDriver(kafkaSourceConf).driver().get
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)
    val kafkaAllLogReader: KafkaAllLogReader = new KafkaAllLogReader(consumer, kafkaSourceConf.uri, kafkaSourceConf.topic.getOrElse("topic"),
      kafkaSourceConf.wildcard, kafkaSourceConf.position, 500,
      kafkaSourceConf.codec, None, None, null)
    val stream = kafkaAllLogReader.stream
    var count = 0
    try {
      while (stream.hasNext) {
        stream.next()
        count = count + 1
      }
    } catch {
      case _: TimeoutException =>
        stream.close()
        kafkaAllLogReader.close()
        assert(count == 150)
      case _: Exception =>
        assert(false)
    }
  }

  //验证指定多个分区topic的情况
  @Test
  def testConsumerPartition(): Unit = {
    val kafkaSourceConf = KafkaSource(hostPorts, Some(topics(3)), None, "false", Some(ProcessFrom.START), None, None)
    val config = KafkaDriver(kafkaSourceConf).driver().get
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)
    val kafkaAllLogReader: KafkaAllLogReader = new KafkaAllLogReader(consumer, kafkaSourceConf.uri, kafkaSourceConf.topic.getOrElse("topic"),
      kafkaSourceConf.wildcard, kafkaSourceConf.position, 500,
      kafkaSourceConf.codec, None, None, null)
    val stream = kafkaAllLogReader.stream
    var count = 0
    try {
      while (stream.hasNext) {
        stream.next()
        count = count + 1
      }
    } catch {
      case _: TimeoutException =>
        stream.close()
        kafkaAllLogReader.close()
        assert(count == 99)
      case _: Exception =>
        assert(false)
    }
  }


  @Test
  def producer(): Unit = {
    val props = new Properties()
    //val TOPIC = "liyj-sage-test"//-out"
    val TOPIC = "sage-test-out"
    props.put("bootstrap.servers", hostPorts)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    for (i <- 1 to 50) {
      val record = new ProducerRecord(TOPIC, "sage", s"sage kafka plugin test $i")
      producer.send(record)
    }
    producer.close()
  }

  @Test
  def producer2() = {
    val props = new Properties()
    val TOPIC = "sage-test"
    props.put("bootstrap.servers", hostPorts)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("partitioner.class", "com.haima.sage.bigdata.etl.reader.MyPartitioner")
    val producer = new KafkaProducer[String, String](props)
    for (i <- 1 to 100) {
      val record = new ProducerRecord(TOPIC, s"sage-$i", s"sage kafka plugin test $i")
      producer.send(record)
    }
    producer.close()
  }
}

//设定依据key将当前这条消息发送到哪个partition的规则
class MyPartitioner() extends Partitioner with Logger {

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = {

    val partitions = cluster.partitionsForTopic(topic)
    val numPartitions = partitions.size
    var partitionNum = 0
    try
      partitionNum = key.asInstanceOf[String].length - 5
    catch {
      case _: Exception =>
        partitionNum = key.hashCode
    }
    logger.info("the message sendTo topic:" + topic + " and the partitionNum:" + partitionNum)
    Math.abs(partitionNum % numPartitions)
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}

