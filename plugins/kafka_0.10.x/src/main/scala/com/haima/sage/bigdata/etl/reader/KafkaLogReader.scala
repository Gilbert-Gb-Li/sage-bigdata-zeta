package com.haima.sage.bigdata.etl.reader

import java.util
import java.util.Properties
import java.util.concurrent.TimeoutException

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{Event, ReadPosition, Stream}
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

class KafkaLogReader(name: String, props: Properties, partitionInfo: PartitionInfo, override val position: ReadPosition, _timeout: Int, codec: Option[Codec], encoding: Option[String] = None) extends LogReader[Event] with Position {

  val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
  final val topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition())

  val stream: Stream[Event] =
    wrap(codec, new Stream[Event](None) with Logger {

      override val loggerName = "com.haima.sage.bigdata.etl.reader.KafkaLogReader$.Stream"


      import scala.collection.JavaConversions._


      consumer.assign(List(topicPartition))
      consumer.seek(topicPartition, position.position + 1)
      var records: util.Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = _
      logger.debug(s" topic:${consumer.assignment()}")

      def query(): Unit = {
        records = consumer.poll(_timeout).iterator()
      }

      final def hasNext: Boolean = {
        state match {
          case State.done | State.fail =>
            if (records != null) {
              records.hasNext
            } else {
              false
            }


          case State.init =>
            if (records != null && records.hasNext) {
              state == State.ready
              true
            } else {
              query()
              if (records == null || !records.hasNext) {
                throw new TimeoutException("kafka load timeout")
              } else {
                true
              }
            }
          case State.ready =>
            true
        }
      }


      override def next(): Event = {

        init()
        val data = records.next()
        position.position = data.offset()
        encoding match {
          case Some(enc) if enc.length > 0 =>
            Event(content = new String(data.value(), enc))
          case _ =>
            Event(content = new String(data.value()))
        }


      }
    })

  def skip(skip: Long): Long = {
    consumer.seek(topicPartition, skip)
    skip
  }
  override def close(): Unit = {
    consumer.close()
    super.close()
  }
  override def path: String = name

}
