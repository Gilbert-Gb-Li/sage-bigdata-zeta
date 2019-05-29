package com.haima.sage.bigdata.etl.server.hander

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.pattern._
import akka.util.Timeout

import scala.language.postfixOps

object TaskManagerUtils {
	private lazy val configStore: ConfigStore = Stores.configStore
	private lazy val modelingStore: ModelingStore = Stores.modelingStore
	private lazy val knowledgeStore: KnowledgeStore = Stores.knowledgeStore
	private lazy val msgStore: TaskLogInfoStore = Stores.taskLogInfoStore
	implicit val timeout = Timeout(30 seconds)
	def isRunningState(currentState: Option[Status.Status]): Boolean =currentState.contains(Status.RUNNING)
	def isStoppedState(currentState: Option[Status.Status]): Boolean =currentState.contains(Status.STOPPED)
	//比较调度操作类型与当前知识库、离线建模和数据通道的状态，判断是否启用本次调度
	def isTargetState(currentState: Option[Status.Status], targetOpt: Option[String]): Boolean ={
		if(isRunningState(currentState) && isTargetOptStart(targetOpt)){
			true
		} else if(isStoppedState(currentState) && isTargetOptStop(targetOpt)){
			true
		} else {
			false
		}
	}
	//任务操作类型是启动
	def isTargetOptStart(targetOpt: Option[String]): Boolean =targetOpt.contains("START")
	//任务操作类型是停止
	def isTargetOptStop(targetOpt: Option[String]): Boolean =targetOpt.contains("STOP")

	/**
		*获取知识库、数据通道离线建模的当前状态
		* @param taskWrapper timerWrapper
		* @param configServer configServer的ActorRef
		* @param modelingServer modelingServer的ActorRef
		* @return Option[Status.Status] 当前知识库、离线建模和数据通道的状态
		*/
	def obtainJobConfigCurrentState(taskWrapper: TaskWrapper, configServer:ActorRef, modelingServer:ActorRef):(String, Option[Status.Status]) ={
		if(taskWrapper.data==null || taskWrapper.data.configId==null || "".equals(taskWrapper.data.configId))
			throw new Exception(s"The data channel| knowledge | modeling scheduling task ${taskWrapper.id} bound is empty")
		taskWrapper.data.jobType match {
			case Some("knowledge")=>
				val knowledge = knowledgeStore.get(taskWrapper.data.configId)
				knowledge match {
					case Some(k)=>
						k.status match {
							case Some("RUNNING")| Some("STARTING")=>(s"知识库：${k.name.getOrElse(k.id)}",Some(Status.RUNNING))
							case _=>(s"知识库：${k.name.getOrElse(k.id)}",Some(Status.STOPPED))
						}
					case None=>
						throw new Exception(s"knowledge $taskWrapper.data.configId do not exist")
				}

			case Some("channel")=>
				val config=Await.result({
					configServer ? (Opt.GET, taskWrapper.data.configId)
				}.asInstanceOf[Future[Option[ConfigWrapper]]], timeout.duration)
				config match {
					case Some(configWrapper)=>
						configWrapper.status match {
							case Status.RUNNING| Status.STARTING | Status.PENDING=>(s"数据通道：${configWrapper.name}",Some(Status.RUNNING))
							case _=>(s"数据通道：${configWrapper.name}",Some(Status.STOPPED))
						}
					case None=>
						throw new Exception(s"Data channel $taskWrapper.data.configId do not exist")
				}


			case _=>
				val modeling=Await.result({
					modelingServer ? (Opt.GET, taskWrapper.data.configId)
				}.asInstanceOf[Future[Option[ModelingWrapper]]],timeout.duration )
				modeling match {
					case Some(modelingWrapper)=>
						modelingWrapper.status  match {
							case Status.RUNNING| Status.STARTING | Status.PENDING=>(s"离线建模：${modelingWrapper.name}",Some(Status.RUNNING))
							case _=>(s"离线建模：${modelingWrapper.name}",Some(Status.STOPPED))
						}
					case None=>
						throw new Exception(s"modeling $taskWrapper.data.configId do not exist")
				}
		}
	}

	/**
		* 启动或是停止数据通道，知识库和离线建模
		* @param remote 数据通道，知识库和离线建模的actorRef
		* @param targetOpt 目标操作类型：停止或是启动
		* @param taskWrapper taskWrapper
		* @return BuildResult
		*/
	def sendRemoteStartStop(remote: ActorRef, targetOpt: Option[String], taskWrapper: TaskWrapper): BuildResult ={

		Await.result({
			remote ? {targetOpt match {
				case Some("START")=>
					if(taskWrapper.data.jobType.contains("knowledge")){
						(Opt.LOAD, taskWrapper.data.configId)
					} else {
						(Opt.START, taskWrapper.data.configId)
					}
				case _=>
					remote ! (Opt.STOP, taskWrapper.data.configId)
			}}
		}.asInstanceOf[Future[BuildResult]], timeout.duration)

	}

	/**
		* 判断依赖的知识库，离线建模和数据通道是否存在
		* @param taskWrapper task实体
		* @return boolean
		*/
	def isExistJobConfig(taskWrapper: TaskWrapper): Boolean ={
		taskWrapper.data.jobType match {
			case Some("channel") =>
				val channels = configStore.get(taskWrapper.data.configId)
				channels != null && channels.nonEmpty
			
			case Some("modeling") =>
				val modelings = modelingStore.get(taskWrapper.data.configId)
				modelings != null && modelings.nonEmpty
			
			case Some("knowledge") =>
				val knowledge = knowledgeStore.get(taskWrapper.data.configId)
				knowledge != null && knowledge.nonEmpty
			
			case _ =>
				false
			
		}
	}

	def updatePropertyTaskName(taskWrapper: TaskWrapper, taskName: Option[String]): TaskWrapper ={
		taskWrapper.copy(data = taskWrapper.data.copy(taskName = taskName))
	}
	def syncPropertyTaskName(taskWrapper: TaskWrapper): TaskWrapper ={
		val taskName = taskWrapper.data.jobType match {
			case Some("channel") =>
				Option(configStore.get(taskWrapper.data.configId).get.name)

			case Some("modeling") =>
				Option(modelingStore.get(taskWrapper.data.configId).get.name)

			case Some("knowledge") =>
				knowledgeStore.get(taskWrapper.data.configId).get.name

			case _ =>None
		}
		updatePropertyTaskName(taskWrapper, taskName)
	}
	def updatePropertyMsg(taskWrapper: TaskWrapper, msg: String): Unit ={
		val timerMsgWrapper =TaskLogInfoWrapper(taskId=taskWrapper.id.get,
			taskType=taskWrapper.data.jobType,configId=Some(taskWrapper.data.configId),
			action=taskWrapper.data.action,msg=Some(msg))
		if(!msgStore.set(timerMsgWrapper))
			throw  new Exception("insert or update schedule status info error")
	}
}
