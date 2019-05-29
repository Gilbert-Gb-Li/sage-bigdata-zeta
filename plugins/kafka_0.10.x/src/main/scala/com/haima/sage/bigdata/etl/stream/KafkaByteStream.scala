package com.haima.sage.bigdata.etl.stream

import java.util
import java.util.Properties

import com.haima.sage.bigdata.etl.common.model.Stream
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.annotation.tailrec


/**
  * Created by zhhuiyan on 2014/11/5.
  */
object KafkaByteStream {
  def apply(props: Properties, partition: PartitionInfo, position: Long, _timeout: Int) = new KafkaByteStream(props, partition, position, _timeout)

  class KafkaByteStream(props: Properties, partition: PartitionInfo, position: Long, _timeout: Int) extends Stream[ConsumerRecord[String, Array[Byte]]](None) {
    private val consumer = new KafkaConsumer[String, Array[Byte]](props)

    import scala.collection.JavaConversions._

    val topicPartition = new TopicPartition(partition.topic(), partition.partition())
    consumer.assign(List(topicPartition))
    consumer.seek(topicPartition, 1l)

    var records: util.Iterator[ConsumerRecord[String, Array[Byte]]] = consumer.poll(_timeout).iterator()

    def query(): Unit = {
      records = consumer.poll(_timeout).iterator()
    }


    @tailrec
    final def hasNext: Boolean = {
      state match {
        case State.done | State.fail =>
          records.hasNext

        case State.init =>
          if (records.hasNext) {
            state == State.ready
            true
          } else {
            query()

            hasNext
          }
        case State.ready =>
          true
      }
    }


    override def next(): ConsumerRecord[String, Array[Byte]] = {

      init()
      records.next()
    }
  }

}

