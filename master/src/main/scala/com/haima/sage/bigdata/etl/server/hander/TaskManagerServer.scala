package com.haima.sage.bigdata.etl.server.hander

import java.util.{Date, Properties, UUID}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.StatusCodes
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store._
import com.haima.sage.bigdata.etl.utils.Logger
import org.quartz._
import org.quartz.impl.triggers.CronTriggerImpl
import org.quartz.impl.{JobDetailImpl, StdScheduler, StdSchedulerFactory}

import scala.util.{Failure, Success, Try}

/**
  * Created by zhhuiyan on 2017/3/28.
  *
  */
class TaskManagerServer extends StoreServer[TaskWrapper, String] with Logger {
	System.setProperty("org.terracotta.quartz.skipUpdateCheck", "true")
	val system: ActorSystem = context.system
	val ssf = new StdSchedulerFactory(initialize())
	private lazy val scheduler: StdScheduler = ssf.getScheduler().asInstanceOf[StdScheduler]
	lazy val store: TaskWrapperStore = Stores.taskStore
	private lazy val storeMsg: TaskLogInfoStore = Stores.taskLogInfoStore

	private lazy val configServer = context.actorOf(Props[ConfigServer])
	private lazy val modelingServer = context.actorOf(Props[ModelingServer])
	private lazy val knowledgeServer = context.actorOf(Props[KnowledgeServer])

	private val JOB_KEY_NAME_SUFFIX_ONE: String = "job_zeta"
	private val TRIGGER_NAME_SUFFIX_ONE: String = "trigger_zeta"
	private val JOB_GROUP_NAME_SUFFIX :String = "zetaGroup"
	private val TRIGGER_GROUP_NAME_SUFFIX :String = "zetaGroup"

	override def receive: Receive = {
		case (remote: ActorRef, configId: String, taskWrapper: TaskWrapper, targetOpt: Option[String]@unchecked,
			originState: Option[Status.Status]@unchecked, retryCount: Int, maxRetryCount: Int) =>
			if(retryCount > maxRetryCount){
				val _taskWrapper = store.get(taskWrapper.id.get).get
				val currentState = TaskManagerUtils.obtainJobConfigCurrentState(_taskWrapper,configServer,modelingServer)
				val d = DateUtils.formatTime(new Date(), null)
				val msg = s"$d\t\t\t\t调度任务${taskWrapper.name}失败次数大于最大尝试次数，${currentState._1}的原始状态是${currentState._2.get},目标操作是[${targetOpt.get}]!"
				TaskManagerUtils.updatePropertyMsg(_taskWrapper, msg)
			} else {
				loopStartStop(remote, configId, targetOpt, originState, taskWrapper, retryCount, maxRetryCount)
			}
		case (action: Option[String]@unchecked, taskWrapper: TaskWrapper, retryCount: Int, maxRetryCount: Int) =>
			//判断依赖的知识库，离线建模和数据通道是否存在
			if(TaskManagerUtils.isExistJobConfig(taskWrapper)){
        val remoteServer = acquireRemoteServer(taskWrapper)
        val originState = TaskManagerUtils.obtainJobConfigCurrentState(taskWrapper,configServer,modelingServer)
        self ! (remoteServer, taskWrapper.data.configId, taskWrapper, action, originState._2, retryCount, maxRetryCount)
		  } else {
				//不存在删除job，更改调度的启用状态为禁用//TODO
				taskWrapper.id match {
					case Some(id)=>
						removeJobFromQueue(id)
						case None=>
				}
      }
		//初始化已有job的调度队列
		case (Opt.SYNC, Opt.INITIAL)  =>
			logger.debug("To start the initialization of the existing scheduling task")
			val jobs = store.all()
			jobs.foreach(wrapper => {
				if(verifyExpression(wrapper))
					if(wrapper.data.enable){
          removeJobFromQueue(wrapper.id.get)
          addJobToQueue(wrapper)
        } else
						removeJobFromQueue(wrapper.id.get)
				else
					removeJobFromQueue(wrapper.id.get)
			})

		case (Opt.SYNC, (Opt.UPDATE, taskWrapper: TaskWrapper)) =>
			if (store.all().exists(schedule =>{
				schedule.name==taskWrapper.name && {if(taskWrapper.id.isDefined)  !schedule.id.contains(taskWrapper.id.get) else true}
			})){
				//"保存数据建模 [{0}] 错误，名称 [{1}] 已存在！"
				sender() ! BuildResult("304","1111",taskWrapper.name,taskWrapper.name)
			}	else{
				//验证cron语句
				val verifyStatus = if( taskWrapper.id.isDefined && !taskWrapper.data.enable &&
															store.get(taskWrapper.id.get).isDefined &&
					taskWrapper.data.cronExpression.equals(store.get(taskWrapper.id.get).get.data.cronExpression))
															true
				                   else verifyExpression(taskWrapper)
				var updateFlag = false
				if(verifyStatus){
					val wrapper = taskWrapper.id match {
						//修改定时任务
						case Some(id) if store.get(id).nonEmpty =>
							//判断时候是修改了Quartz表达式
							if(!taskWrapper.data.cronExpression.equals(store.get(id).get.data.cronExpression))
									updateFlag=true
							TaskManagerUtils.syncPropertyTaskName(taskWrapper)
						//新建定时任务
						case _=>
							TaskManagerUtils.syncPropertyTaskName(taskWrapper.copy(id = Some(UUID.randomUUID().toString)))
					}
					Try{
						if(wrapper.data.enable){ //启用定时任务
							if(updateFlag){
								removeJobFromQueue(wrapper.id.get)
								addJobToQueue(wrapper)
							}
							else
								if(store.get(wrapper.id.get).isEmpty || !store.get(wrapper.id.get).get.data.enable)
										addJobToQueue(wrapper)
						} else {
							removeJobFromQueue(wrapper.id.get)
						}
					} match {
						case Success(_)=>
							if(store.set(wrapper))//"保存任务调度 [{0}] 成功！"
								sender() ! BuildResult(StatusCodes.OK.intValue.toString, "1008", taskWrapper.name)
							else // "保存任务调度 [{0}] 失败！"
								sender() ! BuildResult(StatusCodes.NotModified.intValue.toString, "1010", taskWrapper.name)
						case Failure(e)=>
							e.printStackTrace()//"Quartz表达式无效！"
							sender() ! BuildResult(StatusCodes.NotModified.intValue.toString, "1010", taskWrapper.name)
					}
				}else
					//Quartz表达式无效
					sender()!BuildResult(StatusCodes.NotModified.intValue.toString, "1020", taskWrapper.name)

			}

		case (Opt.SYNC, (Opt.DELETE, id: String)) =>
			// TODO
			val taskWrapper = store.get(id)
			val deleteStatus = removeJobFromQueue(id)
			if(StatusCodes.OK.intValue.toString.equals(deleteStatus.status)){
				if(store.delete(id)){//删除任务调度 [{0}] 成功！
					sender() ! BuildResult(StatusCodes.OK.intValue.toString, "1004", taskWrapper.get.name, taskWrapper.get.name)
				}  else {//删除任务调度 [{0}] 失败！
					sender() ! BuildResult(StatusCodes.NotModified.intValue.toString, "1005", taskWrapper.get.name, taskWrapper.get.name)
				}
			} else {
				sender() ! BuildResult(StatusCodes.NotModified.intValue.toString, "1005", taskWrapper.get.name, taskWrapper.get.name)
			}

			case (Opt.GET,"msg",taskId:String)=>
				sender()!storeMsg.getByTaskId(taskId)
		case obj =>
			logger.info("receive other case !!!")
			logger.debug("sender().path = " + sender().path)
			super.receive(obj)

	}


	/**
	  * 启动或是停止知识库、离线建模或数据通道
	  * @param remote 知识库、离线建模或数据通道的actor引用
	  * @param configId 知识库、离线建模或数据通道id
	  * @param taskWrapper task实体
	  */
	def loopStartStop(remote: ActorRef, configId: String, targetOpt: Option[String],
										originalState: Option[Status.Status]@unchecked, taskWrapper: TaskWrapper,
										retryCount: Int, maxRetryCount: Int): Unit = {
		logger.debug("loop start !!!")
		val currentState = TaskManagerUtils.obtainJobConfigCurrentState(taskWrapper,configServer,modelingServer)
		val d = DateUtils.formatTime(new Date(), null)
		val msg = if(retryCount==0)
			s"$d\t\t\t\t调度任务${taskWrapper.name}被触发，本次触发首次执行${targetOpt.get}${currentState._1}的操作，${currentState._1}的当前状态是${currentState._2.get}。"
		else
			s"$d\t\t\t\t调度任务${taskWrapper.name}当前次的触发操作第${retryCount}次尝试失败重试，执行${targetOpt.get}${currentState._1}的操作，${currentState._1}的当前状态是${currentState._2.get}。"
		TaskManagerUtils.updatePropertyMsg(taskWrapper, msg)
		val triggerKey = new TriggerKey(taskWrapper.id.get + TRIGGER_NAME_SUFFIX_ONE, TRIGGER_GROUP_NAME_SUFFIX)
		Try{
			 TaskManagerUtils.sendRemoteStartStop(remote, targetOpt, taskWrapper)
		} match {
			case Success(buildResult)=>
				if(buildResult.status.equals("200")){//todo
					val newState = TaskManagerUtils.obtainJobConfigCurrentState(taskWrapper,configServer,modelingServer)
					val d = DateUtils.formatTime(new Date(), null)
					val msg = s"$d\t\t\t\t调度任务${taskWrapper.name}执行成功，${newState._1}的当前状态是${newState._2.get}。"
					TaskManagerUtils.updatePropertyMsg(taskWrapper, msg)
				}	else{
					if(!scheduler.isShutdown){
						val nextExecuteTime =  scheduler.getTrigger(triggerKey).getNextFireTime.getTime
						if(nextExecuteTime-System.currentTimeMillis()>(taskWrapper.data.retryInterval.toInt+5)*1000){
							Thread.sleep(taskWrapper.data.retryInterval.toInt*1000)
							self ! (remote, configId, taskWrapper, targetOpt, originalState, retryCount + 1, maxRetryCount)
						}else{
							val d = DateUtils.formatTime(new Date(), null)
							val msg = s"$d\t\t\t\t取消调度任务当前触发job的失败重试操作，即将执行下次触发job的操作。"
							TaskManagerUtils.updatePropertyMsg(taskWrapper, msg)
						}
					}else
						logger.debug("scheduler is shutdown!")

				}
			case Failure(_)=>
				if(!scheduler.isShutdown) {
					val nextExecuteTime =  scheduler.getTrigger(triggerKey).getNextFireTime.getTime
					if (nextExecuteTime - System.currentTimeMillis() > (taskWrapper.data.retryInterval.toInt + 5) * 1000) {
						Thread.sleep(taskWrapper.data.retryInterval.toInt * 1000)
						self ! (remote, configId, taskWrapper, targetOpt, originalState, retryCount + 1, maxRetryCount)
					} else {
						val d = DateUtils.formatTime(new Date(), null)
						val msg = s"$d\t\t\t\t取消调度任务当前触发job的失败重试操作，即将执行下次触发job的操作。"
						TaskManagerUtils.updatePropertyMsg(taskWrapper, msg)
					}
				}else
					logger.debug("scheduler is shutdown!")
		}

	}

	/**
		* 构建调度任务
		* @param taskWrapper task实体
		* @param triggerKey 触发器key
		* @param jobKey jobKey
		* @param cronExpression cron表达式
		* @return BuildResult
		*/
	def buildSingleJob(taskWrapper: TaskWrapper, triggerKey: TriggerKey, jobKey: JobKey, cronExpression: String): BuildResult = {
		Try{
			val jobDetail = new JobDetailImpl()
			jobDetail.setName(jobKey.getName)
			jobDetail.setGroup(jobKey.getGroup)
			jobDetail.setJobClass(classOf[TaskManager])
			val jobDataMap = new JobDataMap()
			jobDataMap.put("taskWrapper", taskWrapper)
			jobDataMap.put("taskServer", self)
			jobDataMap.put("configServer", configServer)
			jobDataMap.put("modelingServer", modelingServer)
			jobDetail.setJobDataMap(jobDataMap)
			val cronTrigger = new CronTriggerImpl()
			cronTrigger.setName(triggerKey.getName)
			cronTrigger.setGroup(triggerKey.getGroup)
			cronTrigger.setCronExpression(cronExpression)
			scheduler.scheduleJob(jobDetail, cronTrigger)
			scheduler.start()
		} match {
			case Success(_)=>BuildResult(StatusCodes.OK.intValue.toString, "1000", taskWrapper.name)
			case Failure(e)=>throw e
		}
	}

	/**
		*  构建调度job
		* @param taskWrapper task实体
		*/
	def addJobToQueue(taskWrapper: TaskWrapper): Unit ={
		//job_zeta+zetaGroup
		val jobKey = new JobKey(taskWrapper.id.get + JOB_KEY_NAME_SUFFIX_ONE, JOB_GROUP_NAME_SUFFIX)
		//trigger_zeta+zetaGroup
		val triggerKey = new TriggerKey(taskWrapper.id.get + TRIGGER_NAME_SUFFIX_ONE, TRIGGER_GROUP_NAME_SUFFIX)
		Try(buildSingleJob(taskWrapper, triggerKey, jobKey,taskWrapper.data.cronExpression)) match {
			case Success(_)=>
				logger.info("Construction of scheduling job success")
				val d = DateUtils.formatTime(new Date(), null)
				val msg = s"$d\t\t\t\t调度任务${taskWrapper.name}，构建成功。"
				TaskManagerUtils.updatePropertyMsg(taskWrapper, msg)
			case Failure(e)=>
				throw e
		}
	}

	/**
	  * 该方法只是用来构建任务而已, 将任务从调度队列中删除
	  * @param jobKey job的唯一标识
	  */
	def deleteJobFromQueue(jobKey: JobKey): Boolean = {
		Try{
			scheduler.deleteJob(jobKey)
			if(scheduler.isStarted&&scheduler.getJobGroupNames.isEmpty){
				logger.debug("there will be the implementation of the job ")
				scheduler.shutdown(true)
			}
		}
		 match{
			case Success(_)=>	true
			case  Failure(e)=>throw e
		}
	}

	/**
	  * 该方法只是用来构建任务而已, 将任务从调度队列中删除
	  * @param id 调度任务id
	  */
	def removeJobFromQueue(id: String): BuildResult = {
		val jobKey = new JobKey(id + JOB_KEY_NAME_SUFFIX_ONE, JOB_GROUP_NAME_SUFFIX)
		val taskWrapper = store.get(id)
		Try(isExistJob(jobKey)) match {
			case Success(_)=>
				taskWrapper match {
					case Some(wrapper) =>
						Try(deleteJobFromQueue(jobKey)) match{
							case Success(_)=>
								BuildResult(StatusCodes.OK.intValue.toString, "1004", wrapper.name, wrapper.name)
							case Failure(e)=>
								e.printStackTrace()
								BuildResult(StatusCodes.NotModified.intValue.toString, "1005", wrapper.name, wrapper.name)
						}
					case None => BuildResult(StatusCodes.OK.intValue.toString, "1005")
				}
			case Failure(_)=>
				BuildResult(StatusCodes.OK.intValue.toString, "1005", taskWrapper.get.name, taskWrapper.get.name)
		}

	}

	/**
	  *
	  * 判断该job在调度队列中是否存在
	  * @param jobKey job的唯一标识
	  */
	def isExistJob(jobKey: JobKey): Boolean = {
		Try(scheduler.checkExists(jobKey)) match {
			case Success(_)=> true
			case Failure(e)=>
			throw e
		}
	}

	/**
		*
		* 验证Quartz表达式
		* @param taskWrapper task
		* @return boolean值，存在true，不存在false
		*/
	def verifyExpression(taskWrapper: TaskWrapper):Boolean={
		if(taskWrapper!=null&&taskWrapper.data!=null&&taskWrapper.data.cronExpression!=null&&
			!"".equals(taskWrapper.data.cronExpression)&&CronExpression.isValidExpression(taskWrapper.data.cronExpression)){
			//创建CronTrigger，指定组及名称,并设置Cron表达式
			val startCronTrigger = new CronTriggerImpl()
			startCronTrigger.setName("startName")
			startCronTrigger.setGroup("startGroup")
			startCronTrigger.setCronExpression(taskWrapper.data.cronExpression)
			//获取trigger首次触发job的时间，以此时间为起点，每隔一段指定的时间触发job
			val firstFireTime=Try(startCronTrigger.computeFirstFireTime(null)	) match {
										case Success(_)=>startCronTrigger.computeFirstFireTime(null)
										case Failure(_)=>null
										}
			val nextFireTime = startCronTrigger.getFireTimeAfter(firstFireTime) match {
				case null=>0
				case _=>startCronTrigger.getFireTimeAfter(firstFireTime).getTime
			}
			if(firstFireTime==null ||(nextFireTime==0 &&  (firstFireTime.getTime - System.currentTimeMillis()) <= 1000*10))
				false
			else
				true
		}else
			false
	}

	/**
		* 获取知识库，离线建模或数据通道对应ActorRef
		* @param taskWrapper task实体
		* @return actorRef
		*/
	def acquireRemoteServer(taskWrapper: TaskWrapper): ActorRef ={
		taskWrapper.data.jobType match {
			case Some("modeling") => modelingServer
			case Some("knowledge") => knowledgeServer
			case _ => configServer
		}
	}

	def initialize():Properties={
		val properties = new Properties()
		properties.setProperty("org.quartz.scheduler.instanceName",Constants.MASTER.getString("org.quartz.scheduler.instanceName"))
		properties.setProperty("org.quartz.scheduler.rmi.export",Constants.MASTER.getString("org.quartz.scheduler.rmi.export"))
		properties.setProperty("org.quartz.scheduler.rmi.proxy",Constants.MASTER.getString("org.quartz.scheduler.rmi.proxy"))
		properties.setProperty("org.quartz.scheduler.wrapJobExecutionInUserTransaction",Constants.MASTER.getString("org.quartz.scheduler.wrapJobExecutionInUserTransaction"))
		properties.setProperty("org.quartz.scheduler.batchTriggerAcquisitionMaxCount",Constants.MASTER.getString("org.quartz.scheduler.batchTriggerAcquisitionMaxCount"))
		properties.setProperty("org.quartz.threadPool.class",Constants.MASTER.getString("org.quartz.threadPool.class"))
		properties.setProperty("org.quartz.threadPool.threadCount",Constants.MASTER.getString("org.quartz.threadPool.threadCount"))
		properties.setProperty("org.quartz.threadPool.threadPriority",Constants.MASTER.getString("org.quartz.threadPool.threadPriority"))
		properties.setProperty("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread",Constants.MASTER.getString("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread"))
		properties.setProperty("org.quartz.jobStore.misfireThreshold",Constants.MASTER.getString("org.quartz.jobStore.misfireThreshold"))
		properties.setProperty("org.quartz.jobStore.class",Constants.MASTER.getString("org.quartz.jobStore.class"))
		properties
	}
}
