package com.haima.sage.bigdata.etl.server.hander

import akka.actor.{ActorRef, ActorSelection, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model.{Channel, _}
import com.haima.sage.bigdata.etl.server.knowlege.KnowledgeStoreManager
import com.haima.sage.bigdata.etl.store._

import scala.concurrent._
import scala.util.{Failure, Success}

/**
  * Created by evan on 17-8-4.
  */
class ModelingServer extends StoreServer[ModelingWrapper, String] {
  lazy val store: ModelingStore = Stores.modelingStore
  private lazy val taskStore = Stores.taskStore
  val statusServer: ActorSelection = context.actorSelection("/user/status-server")
  private lazy val host: String = Constants.CONF.getString("akka.remote.netty.tcp.hostname")
  private lazy val port: Int = Constants.CONF.getInt("akka.remote.netty.tcp.port")


  import context.dispatcher

  override def receive: Receive = {
    case (start: Int, limit: Int, orderBy: Option[String@unchecked], order: Option[String@unchecked], sample: Option[ModelingWrapper@unchecked]) =>
      val data = store.queryByPage(start, limit, orderBy, order, sample)
      sender() ! data.copy(result = data.result.map(setStatus))
    case Opt.GET =>
      sender() ! store.all().map(setStatus)
    case (Opt.GET, analyzerType: AnalyzerType.Type) =>
      //获取所有的模型数据表的表名 KNOWLEDGE_modelingId@analyzerId
      val allModelTables = store.queryModelingTableList()
      //获取所有运行成功的离线建模 modelingId
      val successModelLists = store.queryByType(analyzerType).map(setStatus)
      //modelingId
      val finishedModelLists = successModelLists.map(model=>model.id.getOrElse(""))
      val lists = allModelTables.filter(tableName=>{
       val compositeId =  tableName.toLowerCase.replaceAll("_","-").substring(10)
        compositeId.split("@").length==2 && finishedModelLists.contains(compositeId.split("@")(0))
      }).map(tableName=>{
        val compositeId= tableName.toLowerCase.replaceAll("_","-").substring(10)
        val modelingId = compositeId.split("@")(0)
        val analyzerId = compositeId.split("@")(1)
        val modelingWrapper = store.get(modelingId)
         val analyzerWrapper = analyzerStore.get(analyzerId)
        //判断模型和分析规则是否存在
        if(modelingWrapper.nonEmpty && analyzerWrapper.nonEmpty){
          val showName = modelingWrapper.get.name+"-"+analyzerWrapper.get.name
          modelingWrapper.get.copy(id = Some(compositeId),name = showName)
        }else{
          null
        }
      }).filter(rs=> rs != null)
      sender() ! lists
    case (Opt.CHECK, workerId: String, url: String) =>
      val remoteActor = context.actorSelection("/user/server")
      val actor = sender()
      (remoteActor ? (workerId, (Opt.CHECK, "flink", url))).asInstanceOf[Future[BuildResult]].onComplete {
        case Success(data) =>
          actor ! data
        case Failure(e) =>
          actor ! BuildResult("500", "930", e.getCause.toString)
      }
    case (opt: Opt.Opt, id: String) =>
      store.get(id) match {
        case Some(modelingWrapper) =>
          opt match {
            case Opt.START => // 启动
              try {
                val config = toConfig(modelingWrapper)

                config.worker match {
                  case Some(worker) =>
                    val remoteActor = context.actorSelection("/user/server")
                    val actor = sender()
                    (remoteActor ? (worker.id, (config, Opt.START))).asInstanceOf[Future[BuildResult]].onComplete {
                      case Success(data) =>
                        data.status match {
                          case "304" =>
                            statusServer ! RunStatus(config.id, None, ProcessModel.MONITOR, Status.STOPPED)
                          case _ =>
                        }
                        actor ! data
                      case Failure(e) =>
                        actor ! BuildResult(StatusCodes.NotModified.intValue.toString, "1104", config.id, e.getMessage)
                    }
                  case None =>
                    sender() ! BuildResult(StatusCodes.NotModified.intValue.toString, "1105", config.id)
                }
              } catch {
                case ex: Exception =>
                  logger.error(s"Start ModelingConfig[$id] Error", ex)
              }
            case Opt.STOP => // 停止
              val modeling = store.get(id).orNull
              sender() ! BuildResult(StatusCodes.OK.intValue.toString, "1106", modeling.name)
              try {
                val config = toConfig(modelingWrapper)
                config.worker match {
                  case Some(worker) =>
                    val remoteActor = context.actorSelection("/user/server")
                    val actor = sender()
                    (remoteActor ? (worker.id, (config, Opt.STOP))).asInstanceOf[Future[BuildResult]].onComplete {
                      case Success(data) =>
                        data.status match {
                          case "304" =>
                            statusServer ! RunStatus(config.id, None, ProcessModel.MONITOR, Status.STOPPED)
                          case _ =>
                        }
                        actor ! data
                      case Failure(e) =>
                        actor ! BuildResult(StatusCodes.NotModified.intValue.toString, "1107", config.id, e.getCause.toString)
                    }
                  case None =>
                    sender() ! BuildResult(StatusCodes.NotModified.intValue.toString, "1105", config.id)
                }
              } catch {
                case ex: Exception =>
                  logger.error(s"Stop ModelingConfig[$id] Error", ex)
              }
            case Opt.GET => // 查询
              sender() ! Some(setStatus(modelingWrapper))
            case _ => // 其他操作不容许
              sender() ! BuildResult(StatusCodes.MethodNotAllowed.intValue.toString, "1108", opt.toString)
          }
        case None =>
          sender() ! BuildResult(StatusCodes.MethodNotAllowed.intValue.toString, "1109", id)
      }
    case ("modeling", configId: String, jobId: String) =>
      logger.debug(s"receive modeling flink jobId[$jobId] of config[$configId]")
      store.get(configId) foreach {
        wrapper =>
          store.set(wrapper.copy(`type` = wrapper.`type`.map(_.copy(jobId = jobId))))
      }
    case (Opt.SYNC, (Opt.CREATE, modeling: ModelingWrapper)) =>
      if (store.all().exists(_.name == modeling.name)) {
        sender() ! BuildResult("304", "1111", modeling.name)
      } else {
        if (store.set(modeling.copy(`type` = modeling.`type`.map(d => d.copy(model = AnalyzerModel.MODELING))))) {
          sender() ! BuildResult("200", "1100", modeling.name)
        } else {
          sender() ! BuildResult("200", "1101", modeling.name)
        }
      }
    case (Opt.SYNC, (Opt.UPDATE, modeling: ModelingWrapper)) =>
      if (store.all().exists(ds => ds.name == modeling.name && !ds.id.contains(modeling.id.get))) {
        sender() ! BuildResult("304", "1111", modeling.name, modeling.name)
      } else {
        if (store.set(modeling.copy(`type` = modeling.`type`.map(d => d.copy(model = AnalyzerModel.MODELING))))) {
          sender() ! BuildResult("200", "1100", modeling.name)
        } else {
          sender() ! BuildResult("200", "1101", modeling.name)
        }
      }
    case (Opt.SYNC, (Opt.DELETE, id: String)) =>
      sender() ! delete(id)
    case obj =>
      super.receive(obj)
  }

  def toConfig(wrapper: ModelingWrapper): ModelingConfig = {

    /*当时建模时,启动store 同时 设置存储为转发,并sink类型是forward 到store*/
    val sinks = wrapper.`type`.get.`type` match {
      case AnalyzerType.MODEL =>
        val storer: ActorRef = context.actorOf(Props.create(classOf[KnowledgeStoreManager], wrapper.id.get))

        List(ForwardWriter(wrapper.id.get,
          host,
          port,
          Some("master"),
          Some(storer.path.toString.replaceAll("akka://master", ""))
        ))
      case _ =>
        wrapper.sinks.map(id => writeStore.get(id).map(_.data).orNull)
    }
    val channels = getSingleChannel(wrapper)

    ModelingConfig(
      id = wrapper.id.get,
      channel = channels,
      worker = wrapper.worker.map(id => collectorStore.get(id).orNull),
      sinks = sinks,
      properties = Some(wrapper.properties.getOrElse(Map.empty[String, String]).+("wrapper.name" -> wrapper.name))
    )
  }


  def isStopped(conf: ModelingConfig): Boolean = {

    statusStore.monitorStatus(conf.id) match {
      case None =>
        true
      case Some(Status.STOPPED) =>
        true
      case _ => false

    }
  }

  private def fileCollectorStatus(collector: Collector) = {
    collector.status match {
      case Some(Status.RUNNING) =>
        // 判断当前采集器状态，并获去采集器对象
        collector.copy(status = {
          val future = context.actorSelection("/user/server") ? (collector.id, Opt.CHECK)
          try {
            val result: Status.Status = Await.result(future.map {
              case d: Status.Status =>
                d
              case _ =>
                Status.WORKER_STOPPED
            }, timeout.duration)
            Option(result)
          } catch {
            case e: Exception =>
              logger.debug(s"start server in " + e.printStackTrace())
              Option(Status.WORKER_STOPPED)
          }
        })
      case _ =>
        collector
    }
  }

  def setStatus(wrapper: ModelingWrapper): ModelingWrapper = {
    wrapper.worker match {
      case Some(workerId) =>
        if (collectorStore.get(workerId).nonEmpty) {
          Option(fileCollectorStatus(collectorStore.get(workerId).get)).map(_.status.orNull) match {
            case Some(Status.RUNNING) =>
              statusStore.status(wrapper.id.get) match {
                case runStatuses =>
                  val monitorStatus: (Status.Status, Option[String]) = runStatuses.filter(_.model == ProcessModel.MONITOR) match {
                    case Nil =>
                      (Status.STOPPED, None)
                    case head :: Nil =>
                      (head.value, head.errorMsg)
                    case _ =>
                      (Status.UNKNOWN, None)
                  }
                  val writers = runStatuses.filter(_.model == ProcessModel.WRITER).map(_.value.toString)

                  val writerStatus = if (writers.isEmpty) {
                    Status.STOPPED.toString
                  } else {
                    writers.reduce[String] {
                      case (a, b) =>
                        if (a.contains("ERROR") || b.contains("ERROR")) {
                          "WRITER_ERROR"
                        } else {
                          a
                        }
                    }
                  }
                  if (monitorStatus._1 == Status.RUNNING) {
                    if (writerStatus.contains("ERROR")) {
                      wrapper.copy(status = Status.WRITER_ERROR)
                    } else {
                      wrapper.copy(status = monitorStatus._1, errorMsg = monitorStatus._2)
                    }
                  } else {
                    wrapper.copy(status = monitorStatus._1, errorMsg = monitorStatus._2)
                  }
              }

            case _ =>
              logger.warn(s"worker[${wrapper.worker.orNull}] not running")
              wrapper.copy(status = Status.WORKER_STOPPED)
          }
        } else {
          wrapper.copy(status = Status.WORKER_STOPPED)
        }
      case _ =>
        wrapper
    }

  }

  def getSingleChannel(wrapper: ModelingWrapper): SingleChannel = {

    def getChannel(configId: String, parent: String = ""): Channel = {
      configStore.get(configId) match {
        case Some(configWrapper) =>
          val channel = getChannel(configWrapper.datasource.get, configId)
          configWrapper.parser match {
            case None =>
              channel.asInstanceOf[SingleChannel]
            case Some(parserId) =>
              getAnalyzer(parserId) match {
                case None =>
                  SingleChannel(channel.asInstanceOf[SingleChannel].dataSource,
                    getParser(parserId),
                    Option(configWrapper.id),
                    `type` = configWrapper.`type`
                  )
                case analyzer@Some(_) =>
                  SingleChannel(channel,
                    analyzer,
                    Option(configWrapper.id),
                    `type` = configWrapper.`type`
                  )
              }
          }
        /*configWrapper.parser.map(id => analyzerStore.get(id).map(_.data.orNull).orNull) match {
          case Some(a) if a != null =>
            SingleChannel(channel,
              Some(a),
              Option(configWrapper.id),
              `type` = configWrapper.`type`
            )
          case _ =>

            channel.asInstanceOf[SingleChannel]
        }*/
        case None =>
          // TupleChannel or TableChannel

          getSource(dataSourceStore.get(configId).get.data, parent)
      }
    }

    def getSource(source: DataSource, parent: String = ""): Channel = {
      source match {
        case tuple@TupleChannel(first, second, _, _) =>
          tuple.copy(
            first = first.id.map(ref => getChannel(ref, ref)).getOrElse(first),
            second = second.id.map(ref => getChannel(ref, ref)).getOrElse(second)
          ).withID(parent)
        case tuple@TupleTableChannel(first, second, _) =>
          tuple.copy(
            first = first.id.map(ref => getChannel(ref, ref)).getOrElse(first).asInstanceOf[TableChannel],
            second = second.id.map(ref => getChannel(ref, ref)).getOrElse(second).asInstanceOf[TableChannel]
          ).withID(parent)

        case table@SingleTableChannel(channel, _, _) =>
          table.copy(
            channel = channel.id.map(ref => getChannel(ref, ref)).getOrElse(channel)
          ).withID(parent)
        case single: SingleChannel =>
          single.withID(parent)
        case _ =>
          SingleChannel(source).withID(parent)
      }
    }

    def getParser(id: String): Option[Parser[MapRule]] = {
      parserStore.get(id) match {
        case Some(a) =>
          a.parser
        case _ =>
          None
      }
    }

    def getAnalyzer(id: String): Option[Analyzer] = {
      analyzerStore.get(id) match {
        case Some(a) =>
          a.data.map(_.withId(id))
        case _ =>
          None
      }
    }

    val channel = getChannel(wrapper.channel)
    wrapper.analyzer.map(getAnalyzer) match {
      case Some(a) if a != null =>
        SingleChannel(channel,
          a,
          wrapper.id,
          `type` = wrapper.`type`
        )
      case _ =>
        channel.asInstanceOf[SingleChannel]
    }


  }

  def delete(id: String): BuildResult={
    store.get(id) match {
      case Some(modelingWrapper)=>
        val modelingWrapperName = if("".equals(modelingWrapper.name) || modelingWrapper.name==null) modelingWrapper.id.getOrElse("") else modelingWrapper.name
       if(taskStore.byConf(id).nonEmpty){//被调度任务依赖
          logger.info(s"MODELING[$id] HAS BEEN REFEREED BY TASK MANAGER.")
          BuildResult("304","1112",modelingWrapperName)
        }else if(store.delete(id)){
          logger.info(s"DELETE MODELING[$id] SUCCEED!")
          store.init
          BuildResult(StatusCodes.OK.intValue.toString,"1102",modelingWrapperName)
        }
        else{
          logger.info(s"DELETE MODELING[$id] FAILED!")
          BuildResult(StatusCodes.NotAcceptable.intValue.toString,"1103",modelingWrapperName)
        }
      case None=>
        logger.info(s"DELETE MODELING[$id] FAILED!")
        BuildResult(StatusCodes.NotAcceptable.intValue.toString,"1103",id)
    }

  }
}
