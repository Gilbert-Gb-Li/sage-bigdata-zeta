package com.haima.sage.bigdata.etl.server.knowledge


import akka.actor.{Actor, ActorSelection}
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.knowledge.KnowledgeStore
import com.haima.sage.bigdata.etl.store.Stores

import scala.util.{Failure, Success, Try}
import org.slf4j.{Logger, LoggerFactory}

case class KnowledgeStoreServer(override val operate: String,override val tableSchema:Option[Schema])
  extends KnowledgeStore with Actor  {
  private lazy val logger: Logger = LoggerFactory.getLogger(classOf[KnowledgeStoreServer])
  lazy val server: ActorSelection = context.actorSelection("/user/server")  //master server
  lazy val knowledgeStore: com.haima.sage.bigdata.etl.store.KnowledgeStore= Stores.knowledgeStore
  private var isOperateTable = false //数据表的创建是否完成
  override def preStart(): Unit = {

  }
  override def receive: Receive = {
    case Opt.CREATE=> //表的创建或是更新操作
        if(operate(operate))
          sender()!(Opt.CREATE,Status.SUCCESS)
        else{
          sender()!Status.FAIL
          context.stop(self)
        }
    case (data: List[Any@unchecked],finishedLoadFlag:Boolean) =>
       Try{
         data.foreach {
               case d: Map[String@unchecked, Any@unchecked] =>
                 insert(filterData(d))
               case msg =>
                throw new Exception(s"un handed msg[$msg]")
         }
         commit()

       } match {
         case Success(_) =>
           if(!finishedLoadFlag){
             sender() ! Status.FINISHED
             self ! Status.FINISHED
           }else
             sender() ! Opt.GET
         case Failure(e) =>
           e.printStackTrace()
           sender() ! Status.FAIL
           self ! Status.STOPPED
       }
    case Status.STOPPED=>
      close()
      context.stop(self)

    case Status.FINISHED=>
      // 需要更新知识库
      val kid = table.replace("KNOWLEDGE_", "").replace("_", "-").replace("\"","").toLowerCase
      logger.debug(s"load knowledge[$kid] success !")
      server ! (kid,"KNOWLEDGE","LOAD","FINISHED")
      sender() ! Status.SUCCESS
      self ! Status.STOPPED

      //离线建模
     case data:Map[String@unchecked,Any@unchecked]=>
       Try{
         if(!isOperateTable ){ //如果还没有进行表的创建
           operate(operate)
           isOperateTable=true
          }
         //插入数据
         data match {
           case d: Map[String@unchecked, Any@unchecked] =>
             insert(filterData(d))  //插入数据
           case msg =>
             throw  new Exception(s"un handed msg[$msg]")
         }
       } match {
       case Success(_)=>
         sender() ! Status.SUCCESS
       case Failure(e)=>
         e.printStackTrace()
         sender()!Status.FAIL
         //self ! Status.STOPPED
       }
    case msg=>
      logger.debug(msg.toString)
  }
}
