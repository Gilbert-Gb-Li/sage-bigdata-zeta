package com.haima.sage.bigdata.etl.server.hander

import java.util.Date

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store._
import com.haima.sage.bigdata.etl.utils.Logger
import org.quartz._


class TaskManager extends Job  with Logger {

	private lazy val msgStore: TaskLogInfoStore = Stores.taskLogInfoStore
	
	/**
	  * 系统周期地调用执行该方法
	  * @param quartzContext   quartz下文
	  */
	override def execute(quartzContext: JobExecutionContext): Unit = {
		val taskWrapper = quartzContext.getJobDetail.getJobDataMap.get("taskWrapper").asInstanceOf[TaskWrapper]
		val taskServer = quartzContext.getJobDetail.getJobDataMap.get("taskServer").asInstanceOf[ActorRef]
		val configServer = quartzContext.getJobDetail.getJobDataMap.get("configServer").asInstanceOf[ActorRef]
		val modelingServer = quartzContext.getJobDetail.getJobDataMap.get("modelingServer").asInstanceOf[ActorRef]
		//获取知识库、数据通道离线建模的当前状态
		val currentState  = TaskManagerUtils.obtainJobConfigCurrentState(taskWrapper,configServer,modelingServer)
		//启动还是停止操作
		val targetOpt = taskWrapper.data.action
		//比较调度操作类型与当前知识库、离线建模和数据通道的状态，判断是否启用本次调度
		if(TaskManagerUtils.isTargetState(currentState._2, targetOpt)){
			val d = DateUtils.formatTime(new Date(), null)
			val msg =  s"$d\t\t\t\t${currentState._1}的当前状态是[${currentState._2.getOrElse("STOPPED")}],不能再执行[${targetOpt.get}]类型的任务调度，取消本次调度！"
			val timerMsgWrapper = TaskLogInfoWrapper(taskId=taskWrapper.id.get,
				taskType=taskWrapper.data.jobType,configId=Some(taskWrapper.data.configId),
				action=taskWrapper.data.action,msg=Some(msg))
			if(!msgStore.set(timerMsgWrapper))
				logger.error("insert or update schedule status info error")
		} else {
			logger.debug("crontabJob send message to timerServer before !!!")
			taskServer ! (targetOpt, taskWrapper, 0, taskWrapper.data.retry)
			logger.debug("crontabJob send message to timerServer after !!!")
		}

		
	}
	

	
	
}
