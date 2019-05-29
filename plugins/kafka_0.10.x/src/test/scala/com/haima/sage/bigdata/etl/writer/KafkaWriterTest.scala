package com.haima.sage.bigdata.etl.writer

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, DeadLetter, Props}
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{KafkaWriter, Opt, Status}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import scala.collection.JavaConversions._

import scala.concurrent.duration._

/**
  * Created by zhhuiyan on 15/2/6.
  */
@Test
class KafkaWriterTest {

  implicit val timeout = Timeout(50 seconds)
  //val hostPorts = "10.10.106.69:9092,10.10.106.70:9092,10.10.106.89:9092,10.10.106.90:9092"
  val hostPorts="10.10.106.169:9092,10.10.106.170:9092,10.10.109.89:9092,10.10.106.190:9092"
  val topics = Array("sage-bigdata-etl-test-out","sage-bigdata-etl-test,sage-bigdata-etl-test-out","sage-bigdata-etl-test*","sage-bigdata-etl-test:1,2")

  @Test
  def test(): Unit = {
    val system = ActorSystem("local")
    val writer = system.actorOf(Props(classOf[KafkaLogWriter], KafkaWriter("0", "172.16.219.130:2111", Some("kafka-test")), null), "startActor")

    val deadLetterListener = system.actorOf(Props[KafkaDeadLetterListener])
    system.eventStream.subscribe(deadLetterListener, classOf[DeadLetter])
    writer ! Map[String, Any](
      "test" -> "1",
      "test2" -> "2",
      "test" -> "3",
      "test" -> "4"
    )
    writer ! Opt.STOP
    while (Box1.flag) {
      TimeUnit.SECONDS.sleep(1)
    }

  }

  @Test
  def consumer(): Unit ={
    val props = new Properties()
    val TOPIC = "sage-bigdata-etl-test"
    props.put("bootstrap.servers", hostPorts)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id","sage-bigdata-etl-test3667763")
    props.put(Constants.AUTO_OFFSET_RESET, "earliest")
    //Consumer session 过期时间。这个值必须设置在broker configuration中的group.min.session.timeout.ms 与 group.max.session.timeout.ms之间。
    //其默认值是：10000 （10 s）
    props.put("session.timeout.ms", "30000")
    //请求发起后，并不一定会很快接收到响应信息。这个配置就是来配置请求超时时间的。默认值是：305000 （305 s）
    //request.timeout.ms should be greater than session.timeout.ms and fetch.max.wait.ms
    props.put("request.timeout.ms","60000")
    //心跳间隔。心跳是在consumer与coordinator之间进行的,
    //这个值必须设置的小于session.timeout.ms 常设置的值要低于session.timeout.ms的1/3. 默认值是：3000 （3s）
    props.put("heartbeat.interval.ms","999")
    //当consumer向一个broker发起fetch请求时，broker返回的records的大小最小值。如果broker中数据量不够的话会wait，直到数据大小满足这个条件。
    //取值范围是：[0, Integer.Max]，默认值是1。
    props.put("fetch.min.bytes","0")
    //Fetch请求发给broker后，在broker中可能会被阻塞的（当topic中records的总size小于fetch.min.bytes时），
    //此时这个fetch请求耗时就会比较长。这个配置就是来配置consumer最多等待response多久。
    props.put("fetch.max.wait.ms","1000")
    val consumer = new KafkaConsumer[String, String](props)
    var map:Map[TopicPartition, Long] = Map()
    println(System.currentTimeMillis())

    try{
      val partitions = consumer.partitionsFor(TOPIC)
    partitions.foreach(p =>{
      map += new TopicPartition(p.topic(), p.partition()) -> 0l
    })}catch {
      case e:org.apache.kafka.common.errors.TimeoutException=>
        throw e
      case e:org.apache.kafka.common.errors.WakeupException=>
        throw e
      case e :org.apache.kafka.common.errors.InterruptException=>
        throw e
      case e:org.apache.kafka.common.errors.AuthorizationException=>
        throw e
      case e: org.apache.kafka.common.KafkaException=>
        throw e
      case e:Exception=>
        throw e
    }
    consumer.subscribe(util.Arrays.asList(TOPIC))
    while ( true) {
      val records = consumer.poll(1000)
      import scala.collection.JavaConversions._
      for (record <- records) {
        println(s"partition = ${record.partition} ,offset = ${record.offset}, key = ${record.key}, value = ${ record.value}")
      }
    }
  }

}

object Box1 {
  var flag = true
}

class KafkaDeadLetterListener extends Actor {
  def receive = {
    case DeadLetter(msg: Status.Status, _, _) =>
      Box1.flag = false
    case DeadLetter(msg, _, _) =>

      println(s"FROM CUSTOM LISTENER $msg")

  }
}

