package com.haima.sage.bigdata.etl.reader

import java.util
import java.util.concurrent.TimeoutException
import java.util.regex.Pattern

import akka.actor.ActorSelection
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.ProcessFrom.{END, ProcessFrom}
import com.haima.sage.bigdata.etl.common.model.{Event, Opt, ReadPosition, Stream}
import com.haima.sage.bigdata.etl.driver.TopicUtils
import com.haima.sage.bigdata.etl.utils.{Logger, WildMatch}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}

class KafkaAllLogReader(consumer: KafkaConsumer[Array[Byte], Array[Byte]],
                        name: String,
                        topicStr: String,
                        wildcard: String,
                        positionMark: Option[ProcessFrom],
                        _timeout: Int,
                        codec: Option[Codec],
                        encoding: Option[String] = None,
                        positionServer: Option[ActorSelection],
                        channelId: String)
  extends LogReader[Event] with Position with WildMatch {

  import scala.concurrent.duration._

  implicit val timeout = Timeout(10 seconds)
  var hasClose = false //consumer是否被关闭
  var batchIndex: Long = 0
  var current: scala.collection.mutable.Map[String, ReadPosition] = _
  val batchs: scala.collection.mutable.Queue[(Long, scala.collection.mutable.Map[String, ReadPosition])] = scala.collection.mutable.Queue[(Long, scala.collection.mutable.Map[String, ReadPosition])]()
  val maxTime = Math.max(_timeout, 1000)

  val stream: Stream[Event] =
    wrap(codec, stream = new Stream[Event](None) with Logger {
      override val loggerName = "com.haima.sage.bigdata.etl.reader.KafkaAllLogReader$.Stream"
      try {
        //1.查询数据库,看该topic是否已存在readPosition
        val positions: List[ReadPosition] = positionServer match {
          case Some(server) =>
            Await.result(server ? (Opt.GET, "list", path), timeout.duration).asInstanceOf[List[ReadPosition]]
          case None =>
            List()
        }
        //2.将数据库中的readPosition转化成键值对的形式
        val topicInfoFromDB: Map[TopicPartition, Long] = TopicUtils().buildTopicInfoFromDB(positions)
        if (wildcard.toBoolean) {
          //3.获得所有的topic，并找出符合通配符的topic，并整合数据库中的readPosition
          val pattern: Pattern = toPattern(topicStr.toCharArray)
          var topicMatch: Map[TopicPartition, Long] = Map()
          try {
            val topics: mutable.Map[String, util.List[PartitionInfo]] = consumer.listTopics()
            topics.foreach(topic => {
              if (pattern.matcher(topic._1).find()) {
                topic._2.foreach(partitionInfo => {
                  val topicPartition: TopicPartition = new TopicPartition(topic._1, partitionInfo.partition())
                  //4.如果文件续读且数据库中有相应的readPosition
                  if (positionMark.getOrElse(END) == END && topicInfoFromDB.nonEmpty && topicInfoFromDB.contains(topicPartition)) {
                    topicMatch += (topicPartition -> topicInfoFromDB(topicPartition))
                  } else {
                    topicMatch += (topicPartition -> 0)
                  }
                })
              }
            })
            /*续读*/
            if (topicMatch.nonEmpty) {
              consumer.assign(topicMatch.keySet)
              topicMatch.foreach(info => {
                consumer.seek(info._1, info._2)
              })
            } else
              throw new Exception(s"no topic match pattern $topicStr")
          } catch {
            case e: org.apache.kafka.common.errors.TimeoutException =>
              logger.error(s"throws an TimeoutException:$e")
              throw e
            case e: org.apache.kafka.common.errors.WakeupException =>
              logger.error(s"throws an WakeupException:$e")
              throw e
            case e: org.apache.kafka.common.errors.InterruptException =>
              logger.error(s"throws an InterruptException:$e")
              throw e
            case e: org.apache.kafka.common.errors.AuthorizationException =>
              logger.error(s"throws an AuthorizationException:$e")
              throw e
            case e: org.apache.kafka.common.KafkaException =>
              logger.error(s"throws an KafkaException:$e")
              throw e
            case e: Exception =>
              logger.error(s"throws an Exception:$e")
              throw e
          }
        } else {
          //3.解析用户配置的topic字符  new TopicPartition(p.topic(), p.partition()) -> 0l
          val topicInfo: Map[TopicPartition, Long] = TopicUtils().buildTopicInfoFromUser(topicStr, consumer)
          if (topicInfo.nonEmpty) {
            consumer.assign(topicInfo.keySet) //手动分配分区;对于用户手动指定topic的订阅模式，通过此方法可以分配分区列表给一个消费者
            topicInfo.foreach(info => {
              try {
                /*续读*/
                if (positionMark.getOrElse(END) == END && topicInfoFromDB.nonEmpty) {
                  //4.如果用户选择文件续读且数据库中的readPosition大于用户配置的readPosition
                  if (topicInfoFromDB.contains(info._1) && topicInfoFromDB(info._1) > info._2) {
                    //使用数据库中的offset来消费
                    consumer.seek(info._1, topicInfoFromDB(info._1))
                  } else {
                    consumer.seek(info._1, info._2)
                  }
                } else {
                  //4.否则使用用户配置的position
                  consumer.seek(info._1, info._2)
                }
              } catch {
                case e: Exception =>
                  logger.error(s"kafka consumer for topic:${info._1} seek position throws an exception", e)
                  e.printStackTrace()
                  throw e
              }
            })
          } else {
            //TODO 3.没有配置到的partition会直接不读取
            //没有配置分区和offset的直接从数据库中读取的位置开始读取
            if (positionMark.getOrElse(END) == END && topicInfoFromDB.nonEmpty) {
              consumer.assign(topicInfoFromDB.keySet)
              topicInfoFromDB.foreach(info => {
                consumer.seek(info._1, info._2)
              })
            } else {
              //topic不存在
              throw new Exception(s"no TopicPartition for $topicStr")
            }
          }
        }
        logger.debug(s" topics:${consumer.assignment()}")
      } catch {
        case e: Exception =>
          logger.error(e.getMessage)
          throw e
      }
      var records: util.Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = _

      def query(): Unit = {
        records = consumer.poll(maxTime).iterator()
      }

      final def hasNext: Boolean = {
        state match {
          case State.done | State.fail =>
            records = null
            /* kafka 不需要清理缓存的数据

            if (records != null) {
              records.hasNext
            } else {
              false
            }*/
            false
          case State.init =>
            if (records != null && records.hasNext) {
              state == State.ready
              true
            } else {
              Try {
                query()
              } match {
                case Success(_) =>
                  if (records == null || !records.hasNext) {
                    throw new TimeoutException("kafka load timeout")
                  } else {
                    true
                  }
                case Failure(e) =>
                  e.printStackTrace()
                  throw e
              }
            }
          case State.ready =>
            true
        }
      }


      override def next(): Event = {
        state match {
          case State.done | State.fail =>
          case _ =>
            init()
        }
        val data: ConsumerRecord[Array[Byte], Array[Byte]] = records.next()
        position.position = data.offset()
        position.records = batchIndex
        val uri: String = path + "_" + data.topic() + "_" + data.partition()
        current.put(uri, ReadPosition(uri, batchIndex, data.offset()))
        val bytes = data.value()
        encoding match {
          case Some(enc) if enc.length > 0 =>
            Event(content = new String(bytes, enc))
          case _ =>
            Event(content = new String(bytes))
        }
      }


    })

  override val position: ReadPosition = ReadPosition(name, batchIndex, 0)

  override def take(time: Long, num: Int): (Iterable[Event], Boolean) = {
    batchIndex += 1
    current = new mutable.HashMap[String, ReadPosition]()
    batchs.enqueue((batchIndex, current))
    super.take(time, num)
  }

  override def callback(batch: ReadPosition)(implicit positionServer: ActorSelection): Unit = {
    /*持久化读取到的位置*/
    var c = batchs.dequeue()

    while (c._1 < batch.records) {
      c = batchs.dequeue()
    }
    c._2.foreach(t => {
      super.callback(t._2)
    })
  }

  def skip(skip: Long): Long = {
    0
  }

  override def close(): Unit = {
    if (!hasClose) {
      try {
        consumer.close()
      } catch {
        case e: Exception =>
          logger.warn(s"error msg:${e.fillInStackTrace()}")
      }

      hasClose = true
    }
    super.close()
  }

  override def path: String = s"$channelId:$name"


}
