package com.haima.sage.bigdata.etl.server.hander

/**
  * Created by Dell on 2017-07-26.
  */

import akka.actor.{ActorPath, ActorPaths, ActorRef, Props}
import akka.http.scaladsl.model.StatusCodes
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.FieldType.FieldType
import com.haima.sage.bigdata.etl.common.model.Status.Status
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.knowledge.Knowledge
import com.haima.sage.bigdata.etl.server.knowledge.KnowledgeStoreServer
import com.haima.sage.bigdata.etl.store._

import scala.util.{Failure, Success, Try}

class KnowledgeServer extends StoreServer[Knowledge, String] {

  lazy val store: KnowledgeStore = Stores.knowledgeStore
  private lazy val taskStore = Stores.taskStore
  private lazy val server = context.actorSelection("/user/server")


  private lazy val url:String={
    Constants.CONF.getString("akka.remote.netty.tcp.hostname")+":"+
    Constants.CONF.getString("akka.remote.netty.tcp.port")
  }
  override def receive: Receive = {
    case (Opt.LOAD,id:String)=> //知识库加载，从外部数据字典加载到知识库表的过程
      val knowledge: Knowledge =store.get(id).orNull
      if(!knowledge.status.contains(Status.RUNNING.toString)){ //如果是非运行状态启动加载知识库
        Try{
          knowledge.datasource.map(dataSourceStore.get).map(_.map(_.data).orNull) match {
            case Some(ds) =>
              //val table="\"" + s"KNOWLEDGE_"+knowledge.id.replace("-","_").toUpperCase()+ "\""
              val table= s"KNOWLEDGE_"+knowledge.id.replace("-","_").toUpperCase()
              val p:Parser[MapRule]=knowledge.parser.map(id=>parserStore.get(id).map(_.parser.orNull).orNull).orNull
              val parserMeta = p.metadata match {
                case Some(x) =>
                  if(x.nonEmpty) true else false
                case None => false
              }
              if (!parserMeta) {
                sender() ! BuildResult("304", "220", "")
              }else{
                val store: ActorRef = context.actorOf(Props.create(classOf[KnowledgeStoreServer],"CREATE",Some(schema(table,p.metadata))))
                val path: ActorPath = ActorPaths.fromString(store.path.toString.replaceAll("akka://master",s"akka.tcp://master@$url"))
                logger.debug(s"server in[${store.path.toString}] actor.path is [$path]")
                knowledge.collector match {
                  case Some(collectorId) =>
                    /*远程模式*/
                    logger.debug("knowledge id: "+knowledge.id)
                    collectorStore.get(collectorId) match {
                      case Some(collector) if collector.status.contains(Status.RUNNING)  => //判断采集器的运行状态，运行状态启动知识库
                        server.forward(collectorId, (ds,p, path, knowledge))
                      case _ =>
                        val collector = collectorStore.get(collectorId).orNull
                        sender() ! BuildResult("304", "800",if(collector==null) "" else collector.name)
                    }
                  case None =>
                    sender() ! BuildResult("304", "520", knowledge.name.getOrElse(knowledge.id))
                  /*本地模式*/
                  /* context.actorOf(Props.create(classOf[KnowledgeLoadServer], ds, path))
                   store.set(knowledge.copy(status=Option(Status.RUNNING.toString)))
                   sender() ! BuildResult("200", "514",self.path.address.toString,ds.toString)*/
                }
              }
            case _ =>
              sender() ! BuildResult("304","113",knowledge.datasource.orNull)
          }
        } match {
          case Success(_)=>
          case Failure(e)=>
            logger.error(e.getMessage)
            sender() ! BuildResult("304", "800","")
        }

      }else{
        sender() ! BuildResult("200","516", knowledge.name.orNull)
      }
    case (Opt.STOP,id:String)=> //停止知识库加载
      val knowledge =store.get(id)
      knowledge match {
        case Some(k)=>
          if(k.status.contains(Status.RUNNING.toString)
          || k.status.contains(Status.STARTING.toString)
          ||k.status.contains(Status.STOPPING.toString) ) { //如果是运行状态和正在停止和正在启动，正在其中状态均可以停止
            k.collector match {
              case Some(collectorId) =>
                collectorStore.get(collectorId) match {
                  case Some(collector) if collector.status.contains(Status.RUNNING) => //判断采集器的运行状态，运行状态启动知识库
                    server.forward(collectorId, (k, Opt.STOP))
                  case None => //采集器没有运行
                    sender() ! BuildResult("304", "520", k.name.getOrElse(k.id))
                }
              case None => //采集器不存在
                sender() ! BuildResult("304", "520", k.name.getOrElse(k.id))
            }
          }else //知识库是非运行状态
            sender() ! BuildResult("304", "522", k.name.getOrElse(k.id))
        case None=> //知识库不存在
          sender() ! BuildResult("304", "523",id)
      }


    case (Opt.SYNC, (Opt.DELETE, id: String)) =>
      sender() ! delete(id)
    case (Opt.SYNC, "queryByStatus") =>
      sender() ! store.queryByStatus()
    case (Opt.SYNC, (Opt.UPDATE, data: Knowledge))=>
      if (store.all().exists(ds => ds.name.orNull==data.name.orNull && !ds.id.contains(data.id))){
        sender() ! BuildResult("304","510",data.name.orNull,data.name.orNull)
      }
      else{
        if(store.set(data)){
          sender() ! BuildResult("200","508",data.name.orNull)
        }
        else{
          sender() ! BuildResult("304","509",data.name.orNull)
        }
      }

    case (start: Int, limit: Int, orderBy: Option[String@unchecked], order: Option[String@unchecked], sample: Option[Knowledge@unchecked]) =>
      val data = store.queryByPage(start, limit, orderBy, order, sample)
      implicit val cStatuses: Map[String, Status.Status] = workerStatus(data.result.map(conf => {
        conf.collector
      }).filter(_.isDefined).map(_.get).groupBy(d => d).keys.toList)
      sender() ! data.copy(result = data.result.map(c => setStatus(c)))
    case msg =>
      super.receive(msg)
  }

  def workerStatus(ids: List[String]): Map[String, Status] = ids.map(id =>
    (id, collectorStore.get(id).map(_.status.map {
      case Status.STOPPED =>
        Status.WORKER_STOPPED
      case status =>
        status
    }.getOrElse(Status.WORKER_STOPPED)).
      getOrElse(Status.WORKER_STOPPED))).
    toMap

  def setStatus(wrapper: Knowledge)(implicit cStatus: Map[String, Status.Status]): Knowledge = {
    wrapper.collector match {
      case Some(id) =>
        cStatus.getOrElse(id, Status.WORKER_STOPPED) match {
          case Status.RUNNING =>
           wrapper
          case status =>
            wrapper.copy(status = Some(status.toString))
        }
      case _ =>
        wrapper.copy(status = Some(Status.UNAVAILABLE.toString))
    }
  }



  def delete(id: String): BuildResult={
    store.get(id) match {
      case Some(knowledge)=>
        val knowledgeName = knowledge.name match {
          case Some(name)=>name
          case None=>id
        }
        if (parserStore.byKnowledge(id).nonEmpty) {
          logger.info(s"KNOWLEDGE[$id] HAS BEEN REFEREED BY PARSER.")
          BuildResult("304","518",knowledgeName)
        }else if(taskStore.byConf(id).nonEmpty){//被调度任务依赖
          logger.info(s"KNOWLEDGE[$id] HAS BEEN REFEREED BY TASK MANAGER.")
          BuildResult("304","526",knowledgeName)
        }else if(store.delete(id)){
          logger.info(s"DELETE KNOWLEDGE[$id] SUCCEED!")
          store.init
          BuildResult(StatusCodes.OK.intValue.toString,"504",knowledgeName)
        }
        else{
          logger.info(s"DELETE KNOWLEDGE[$id] FAILED!")
          BuildResult(StatusCodes.NotAcceptable.intValue.toString,"505",knowledgeName)
        }
      case None=>
        logger.info(s"DELETE KNOWLEDGE[$id] FAILED!")
        BuildResult(StatusCodes.NotAcceptable.intValue.toString,"505",id)
    }

  }

  def schema(name:String, metadata:Option[List[(String, String, String)]]):Schema = {
    val fields = metadata.map(_.filterNot(_._1 == "RAW")) match {
      case Some(data) if data.nonEmpty =>
        data.map {
          case (name: String, vType: String, _: String) =>
            val stat:FieldType = vType.toLowerCase match {
              case "object" =>VARCHAR(2048)
              case "integer" | "long"| "byte"|"short"=>NUMERIC()
              case "string" =>VARCHAR(1024)
              case "float" | "double"=> DECIMAL()
              case "boolean" =>FieldType.BOOLEAN
              case "date" | "time" | "datetime" =>FieldType.TIMESTAMP
              case _ =>VARCHAR(250)
            }
            Field(name, stat)
        }.toArray
      case _ =>
        throw new Exception("jdbc table properties must be set, cause by parser preview may be not click!")
    }
    Schema(name,fields)
  }
}

