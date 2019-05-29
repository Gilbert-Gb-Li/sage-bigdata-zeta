package com.haima.sage.bigdata.etl.driver.usable

import java.util
import java.util.Properties

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{KafkaSource, KafkaWriter, Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.{KafkaDriver, KafkaMate}
import org.apache.kafka.clients.admin.{AdminClient, DescribeClusterOptions, ListTopicsOptions, ListTopicsResult}
import com.haima.sage.bigdata.etl.utils.{Logger, WildMatch}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class KafkaUsabilityChecker(mate: KafkaMate) extends UsabilityChecker with Logger  with WildMatch{

  val driver = KafkaDriver(mate)
  val msg: String = mate.uri + "error:"

  val containsTopicSet :util.Set[String] = new util.HashSet[String]() //已经存在的topic列表
  val noContainsTopicSet :util.Set[String] = new util.HashSet[String]()// 不存在的topic列表
  val errorTopicSet :util.Set[String] = new util.HashSet[String]() //错误的topic配置列表
  val errorWildMatchTopicSet :util.Set[String] = new util.HashSet[String]() //错误的topic配置列表
  val checkResultMap : util.Map[Int,Usability] = new util.HashMap[Int,Usability]()  //校验的结果map,[错误码，Usability]
  override def check: Usability = {
    driver.driver() match {
      case Success(props) =>
        mate.hostPorts.split(",").foreach(address => {
          if(! (checkResultMap.containsKey(400) || checkResultMap.containsKey(401)||checkResultMap.containsKey(201))) //topic校验通过，继续集群连接校验
            if( !checkResultMap.containsKey(200)) //集群地址没有一个可用的情况下，继续校验
              address.split(":").toList match {
            case _ :: second :: _ if second.matches("\\d+") =>
              var client: AdminClient = null
              try {
//                val props = new Properties()
//                props.put("bootstrap.servers", address)
                client = AdminClient.create(props)
                val connectTimeout =  mate.properties match {
                  case Some(properties)=>
                    properties.getProperty("connectTimeout",s"${Constants.CLUSTER_CONNECT_TIMEOUT}").toInt
                  case None =>
                    Constants.CLUSTER_CONNECT_TIMEOUT
                }
                val cluster = client.describeCluster(new DescribeClusterOptions().timeoutMs(Math.max(connectTimeout,Constants.CLUSTER_CONNECT_TIMEOUT)))
                while (!cluster.clusterId().isDone) {  }//等待请求完成
                logger.info("The request has been completed!")
                if (!cluster.clusterId().isCompletedExceptionally){
                  //校验topic信息
                  if(mate.topic.nonEmpty){
                    val checkResult =  mate match {
                      case source:KafkaSource=>
                        checkSourceTopics(mate.topic.get,client,source.wildcard.toBoolean)

                      case writer:KafkaWriter=>checkWriterTopics(writer.topic.get,client)
                    }
                    if(checkResult){
                      val usability =  Usability()
                      checkResultMap.put(200,usability)
                      usability
                    }else{
                      if(! errorWildMatchTopicSet.isEmpty){
                        val falseCause =s"Regular[$errorWildMatchTopicSet] matching does not correspond to the corresponding topic"
                        val usability =  Usability(usable = false,cause =falseCause )
                        checkResultMap.put(201,usability)
                      } else if(! noContainsTopicSet.isEmpty){
                        val falseCause =s"Partial topic does not exist in Kafka cluster, topics：$noContainsTopicSet"
                        val usability =  Usability(usable = false,cause = falseCause)
                        checkResultMap.put(201,usability)
                        usability
                      }else {
                        val falseCause = s"Topic configuration item configuration error, in which the error item is ${errorTopicSet.toString}"
                        val usability = Usability(usable = false, cause =falseCause )
                        checkResultMap.put(400, usability)
                        usability
                      }
                    }
                  }else{
                    val usability =  Usability(usable = false, cause = "The topic configuration item cannot be empty")
                    checkResultMap.put(401,usability)
                    usability
                  }
                }
                else{
                  logger.error(s"error 1:"+cluster.controller().get())


                  val usability =  Usability(usable = false, cause = "The connection to the Kafka cluster failed. Please check whether the cluster address or the Kafka service is available")
                  checkResultMap.put(402,usability)
                  usability
                }
              } catch {
                case e: Exception =>
                  e.printStackTrace()
                  val usability =  Usability(usable = false, cause = "The connection to the Kafka cluster failed. Please check whether the cluster address or the Kafka service is available")
                  checkResultMap.put(402,usability)
                  usability
              } finally {
                if (client != null)
                  client.close()
              }
            case _ =>
              val usability =  Usability(usable = false, cause = "Kafka cluster address configuration error,eg:IP:PORT,IP:PORT")
              checkResultMap.put(403,usability)
              usability
          }
        })
        val usability = if(checkResultMap.containsKey(400) || checkResultMap.containsKey(401)||checkResultMap.containsKey(201)){//存在topic配置错误的情况,或部分topic不存在，校验失败
          if(checkResultMap.containsKey(400))
            checkResultMap.get(400)
          else if(checkResultMap.containsKey(401))
            checkResultMap.get(401)
          else
            checkResultMap.get(201)
        }else if(checkResultMap.containsKey(200)){ //校验成功，有一个可用则可用,否则不可用
           checkResultMap.get(200)
        }else{ //校验失败
          if(checkResultMap.containsKey(402)) checkResultMap.get(402) else checkResultMap.get(403)
        }
        usability
       //有一个可用则可用,否则不可用
      case Failure(e) =>
        Usability(usable = false, cause = e.getMessage)
    }
  }

  /**
    *
    * @param topicsStr topic配置的字符串
    * @param client kafka集群客户端
    * @param wildcard 是否启用正则匹配
    * @return 校验否通过
    */
  def checkSourceTopics(topicsStr:String,client:AdminClient,wildcard:Boolean):Boolean={
    //获取kafka集群中topic的list
    val options = new ListTopicsOptions
    options.listInternal(true)// includes internal topics such as __consumer_offsets
    val topics: ListTopicsResult = client.listTopics(options)
    val topicNames: util.Set[String] = topics.names.get
    logger.debug("Current topics in this cluster: " + topicNames)
    //判断是否支持正则
    if(wildcard){//支持正则匹配
      val pattern = toPattern(topicsStr.toCharArray)
      val rt: Seq[Boolean] =  topicNames.map(topic => {
        if (pattern.matcher(topic).find())
          true
        else
          false
      }).filter(_==true).toList
      if(rt.nonEmpty)
        true
      else{//没有符合正则的topic信息存在
        errorWildMatchTopicSet.add(topicsStr)
        false
      }
    }else{
      val topics: Array[String] =topicsStr.split(";")
      val checkResults: Array[Boolean] = topics.map(topic=>{
        //判断是否指定了分区
        if(!topic.contains(":")){  //单个topic校验,不指定分区校验  例如:sage-bigdata-etl
          if(topicNames.contains(topic)){
            containsTopicSet.add(topic)
            true
          }else{
            noContainsTopicSet.add(topic)
            false
          }
        }else{
          //判断是否指点了多个分区
          val topicInfo: Array[String] = topic.split(":")
          if(topicInfo.length>1){
            if(topicInfo(1).endsWith(",")|| topicInfo(1).endsWith("，")){//例如：sage-bigdata-etl :1,3,
              errorTopicSet.add(topic)
              false
            }else{
              val partitionList = topicInfo(1).split(",").map(pl=>{
                (Try(pl.split("-")(0).toLong).isFailure || pl.split("-")(0).toLong<0   //例如：sage-bigdata-etl:b,sage-bigdata-etl:-1
                  ||(pl.contains("-")&& (topicInfo(1).split("-").length==1 //例如：sage-bigdata-etl:1-
                  || Try(pl.split("-")(1).toLong).isFailure || pl.split("-")(1).toLong<0 )) //例如：sage-bigdata-etl:1-b
                  )
              } ).filter(_==true)
              if(partitionList.nonEmpty) { //错误的配置  例如：sage-bigdata-etl:1,b 或者 sage-bigdata-etl:1-0,2-b
                errorTopicSet.add(topic)
                false
              }else{
                if(topicNames.contains(topicInfo(0))){
                 // fixed  目前没有针对分区级别进行验证
                  containsTopicSet.add(topicInfo(0))
                  true}
                else{
                  noContainsTopicSet.add(topicInfo(0))
                false}
              }
            }
          }else{//错误的topic配置  例如: sage-bigdata-etl:
            errorTopicSet.add(topic)
            false
          }
        }
      })
      if(!checkResults.contains(false))
        true
      else
        false
    }

  }

  /**
    *
    * @param topicsStr topic配置想
    * @param client kafka client
    * @return 校验的结果
    */
  def checkWriterTopics(topicsStr:String,client:AdminClient):Boolean= {
    //获取kafka集群中topic的list
    val options = new ListTopicsOptions
    options.listInternal(true)
    // includes internal topics such as __consumer_offsets
    val topics = client.listTopics(options)
    val topicNames: util.Set[String] = topics.names.get
    logger.debug("Current topics in this cluster: " + topicNames)
    if(topicNames.contains(topicsStr)){
      containsTopicSet.add(topicsStr)
      true
    }else{
      noContainsTopicSet.add(topicsStr)
      false
    }


  }
}