package com.haima.sage.bigdata.etl.driver

import java.util

import com.haima.sage.bigdata.etl.common.model.ReadPosition
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConversions._

/**
  * Created by zhhuiyan on 2017/2/17.
  */
object TopicUtils {
  def apply(): TopicUtils = new TopicUtils

  class TopicUtils extends Logger {
    /**
      * 根据用户填写的topicStr构建可用的分区列表
      */
    def buildTopicInfoFromUser(topicStr: String, consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Map[TopicPartition, Long] = {
      try {
        var map: Map[TopicPartition, Long] = Map()
        if (topicStr.contains(";")) {
          //配置多个topic
          val topicInfo: Array[String] = topicStr.split(";")
          topicInfo.map(s => {
            //topic配置的分区
            if (s.contains(":")) {
              buildSingleTopicInfo(s)
              //不配置分区
            } else {
              val parts: util.List[PartitionInfo] = consumer.partitionsFor(s)
              if (parts != null)
                parts.foreach(p => {
                  map += new TopicPartition(p.topic(), p.partition()) -> 0l
                })
              map
            }
          })
            .reduce((left, right) => left ++ right)
        } else if (topicStr.contains(":")) {
          buildSingleTopicInfo(topicStr)
        } else {
          val parts = consumer.partitionsFor(topicStr)
          if (parts != null)
            parts.foreach(p => {
              map += new TopicPartition(p.topic(), p.partition()) -> 0l
            })
          map
        }
      } catch {
        case e: org.apache.kafka.common.errors.TimeoutException =>
          logger.error(s"buildTopicInfoForUser() throws an TimeoutException:$e")
          throw e
        //Map()
        case e: org.apache.kafka.common.errors.WakeupException =>
          logger.error(s"buildTopicInfoForUser() throws an WakeupException:$e")
          throw e
        //Map()
        case e: org.apache.kafka.common.errors.InterruptException =>
          logger.error(s"buildTopicInfoForUser() throws an InterruptException:$e")
          throw e
        //Map()
        case e: org.apache.kafka.common.errors.AuthorizationException =>
          logger.error(s"buildTopicInfoForUser() throws an AuthorizationException:$e}")
          throw e
        //Map()
        case e: org.apache.kafka.common.KafkaException =>
          logger.error(s"buildTopicInfoForUser() throws an KafkaException:$e}")
          throw e
        //Map()
        case e: Exception =>
          logger.error(s"buildTopicInfoForUser() throws an Exception:$e")
          throw e
        //Map()
      }
    }

    /**
      * 对单条形如"topic:1-2000,2-1000"构建可用的分区列表
      */
    def buildSingleTopicInfo(topicStr: String): Map[TopicPartition, Long] = {
      val topicInfo: Array[String] = topicStr.split(":")
      val topic = topicInfo(0)
      val other = topicInfo(1)
      val partitionInfoList: List[String] = other.split(",").toList
      partitionInfoList.map(pil => {
        if (pil.contains("-")) {
          val partitionInfo: Array[String] = pil.split("-")
          val partition = partitionInfo(0)
          val position = partitionInfo(1)
          new TopicPartition(topic, partition.toInt) -> position.toLong
        } else {
          new TopicPartition(topic, pil.toInt) -> 0l
        }
      }).toMap
    }

    /**
      * 从内置的数据库中构建可用的分区列表
      */
    def buildTopicInfoFromDB(positions: List[ReadPosition]): Map[TopicPartition, Long] = {
      try {
        positions.map(position => {
          val uri = position.path.split("_").reverse
          if (uri.length > 1) {
            val topic: String = uri.slice(1, until = uri.length - uri.length / 2).reverse.mkString("_")
            val partition: Int = uri(0).toInt
            logger.debug(s"reload from topic:$topic ,partition:$partition")
            new TopicPartition(topic, partition) -> (position.position + 1)
          } else {
            null
          }
        }).filterNot(_ == null).toMap[TopicPartition, Long]
      } catch {
        case e: Exception =>
          logger.error(s"buildTopicInfoForDB() throws an Exception:${e.getCause}")
          throw e
      }
    }
  }

}
