package com.haima.sage.bigdata.etl.server.hander


import akka.actor.{ActorRef, ActorSelection}
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.Status.Status
import com.haima.sage.bigdata.etl.common.model.filter.ByKnowledge
import com.haima.sage.bigdata.etl.common.model.{ConfigWrapper, Status, _}
import com.haima.sage.bigdata.etl.store._

import scala.concurrent._
import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 16/7/18.
  */
class ConfigServer extends StoreServer[ConfigWrapper, String] {

  import context.dispatcher

  private lazy val server = context.actorSelection("/user/server")
  private val statusServer: ActorSelection = context.actorSelection("/user/status-server")

  override val store: ConfigStore = Stores.configStore
  private lazy val modelingStore = Stores.modelingStore
  private lazy val taskStore = Stores.taskStore

  override def receive: Receive = {
    //获取config
    case ("getConfig", cw: ConfigWrapper) =>
      sender() ! toConfig(cw)
    //分析启动后更新flink jobId到analyzer
    case (Opt.SYNC, (Opt.UPDATE, configId: String, jobId: String)) =>
      store.get(configId) match {
        case Some(wrapper) =>
          //http://localhost:8081/#/jobs/6c5a74d1e2054d22c778bf73eec4ddb6
          self ! (Opt.UPDATE, wrapper.copy(`type` = wrapper.`type`.map(_.asInstanceOf[AnalyzerChannel].copy(jobId = jobId))))
        case None =>
          logger.warn(s"no config for configId:$configId")
      }

    case (start: Int, limit: Int, orderBy: Option[String@unchecked], order: Option[String@unchecked], sample: Option[ConfigWrapper@unchecked]) =>
      val data = store.queryByPage(start, limit, orderBy, order, sample)

      implicit val cStatuses: Map[String, Status.Status] = workerStatus(data.result.map(conf => {
        conf.collector
      }).filter(_.isDefined).map(_.get).groupBy(d => d).keys.toList)
      sender() ! data.copy(result = data.result.map(c => setStatus(c)))
    case (Opt.GET, id: String) =>
      sender() ! store.get(id).map(info => {

        setStatus(info)(workerStatus(info.collector.toList))
      })
    case ("usable", id: String) =>
      store.get(id) match {
        case Some(wrapper) =>
          server.forward(wrapper.collector.orNull, ("usable", toConfig(wrapper)))
        case _ =>
          sender() ! BuildResult("304", "711")
        //sender() ! Result("304", s"channel[$id] not found!")
      }

    case (Opt.RESTART, data: DataSourceWrapper) =>

      store.all().filter(_.datasource.contains(data.id)).map(wrapper => (wrapper.id, setStatus(wrapper)(workerStatus(wrapper.collector.toList))))


    case (Opt.SYNC, (Opt.CREATE, config: ConfigWrapper)) =>
      if (store.all().exists(_.name == config.name)) {
        sender() ! BuildResult("304", "709", config.name, config.name)
        //sender() ! Result("304", message = s"YOUR SET channel[${config.name}] has exist in system please rename it!")
      } else {
        if (store.set(config.copy(`type` = config.`type`.map {
          case r: AnalyzerChannel =>
            r.copy(model = AnalyzerModel.STREAMING, jobId = "")
          case a =>
            a
        }))) {
          sender() ! BuildResult("200", "700", config.name)
          //sender() ! Result("200", message = s"add channel[${config.id}] success!")
        } else {
          sender() ! BuildResult("200", "701", config.name)
          //sender() ! Result("304", message = s"add channel[${config.id}] failure!")
        }
      }

    case (Opt.RESET, id: String) =>
      store.get(id) match {
        case Some(info) =>
          dataSourceStore.get(info.datasource.get) match {
            case Some(dsinfo) =>
              //重置数据通道的时候需要删除对应status_info
              statusStore.deleteByConfig(info.id)
              context.actorSelection("/user/server").forward((info.collector.get, (info.id, Opt.RESET)))
              sender() ! BuildResult("200", "728")
            //sender() ! Result("200", message = s"Reset Success")
            case _ =>
              BuildResult(StatusCodes.NotFound.intValue.toString, "711", info.name)
            //Result(StatusCodes.NotFound.intValue.toString, StatusCodes.NotFound.reason + s"datasource[$id] not found")
          }
        case _ =>
          BuildResult(StatusCodes.NotFound.intValue.toString, "711")
        //Result(StatusCodes.NotFound.intValue.toString, StatusCodes.NotFound.reason + s"channel[$id] not found")
      }

    case (Opt.SYNC, (Opt.UPDATE, config: ConfigWrapper)) =>
      if (store.all().exists(ds => ds.name == config.name && !ds.id.contains(config.id))) {
        sender() ! BuildResult("304", "709", config.name, config.name)
        //sender() ! Result("304", message = s"your set channel[${config.name}] has exist in system please rename it!")
      } else {
        if (store.set(config.copy(`type` = config.`type`.map {
          case r: AnalyzerChannel =>
            r.copy(model = AnalyzerModel.STREAMING)
          case a =>
            a
        }))) {
          //val config = toConfig(channel)
          sender() ! BuildResult("200", "702", config.name)
          //sender() ! Result("200", message = s"update channel[${config.id}] success!")
          //logger.debug(s"update config $config")
          //  configServer.forward((Opt.UPDATE, config))
        } else {
          sender() ! BuildResult("304", "703", config.name)
          //sender() ! Result("304", message = s"update channel[${config.id}] failure!")
        }
      }
    case (Opt.START, id: String, _: ActorRef) =>

      store.byCollector(id).foreach(info => {
        val status = statusStore.monitorStatus(info.id)
        // logger.debug(s"check:" + info.id + ",status:" + status)
        status match {
          case Some(Status.RUNNING) =>
            start(id, info)
          case Some(Status.STOPPED) =>
          case _ =>
            logger.debug(s"set ${info.id} status to stopped ")
            statusServer ! RunStatus(info.id, None, ProcessModel.MONITOR, Status.STOPPED)
        }

      })
    //启动和停止pipeline
    case (opt: Opt.Opt, id: String) if opt == START || opt == STOP =>
      operate(opt, id)(None)
    /*//更新pipeline
    case (Opt.SYNC, (UPDATE, config: Config)) =>
      val rt: Result = store.get(config.id) match {
        case Some(_config) =>
          if (store.set(ConfigWrapper(config.id, config.collector.get.id, config, "PENDING"))) {
            Result("200", s"update config[${_config.id}] succeed!")
          } else {
            Result("500", s"update config[${_config.id}] failure,please retry!")
          }
        case _ =>
          Result(StatusCodes.NotModified.intValue.toString, s"CONFIG[${config.id}] NOT FOUND, MAYBE YOU WANT CREATE ONE PLEASE USE CREATE API !")
      }
      if (notBroadcast)
        sender() ! rt
    case (Opt.SYNC, (CREATE, config: Config)) =>
      val rt: Result = if (store.set(ConfigWrapper(config.id, config.collector.get.id, config, "PENDING"))) {
        Result("200", s"update config[${config.id}] succeed!")
      } else {
        Result("500", s"update config[${config.id}] failure,please retry!")
      }
      if (notBroadcast)
        sender() ! rt*/
    case (Opt.SYNC, (DELETE, id: String)) =>
      val config = store.get(id).orNull
      val name = if (config == null) "" else config.name
      //校验数据通道是否被数据源绑定，主要是组合通道和单sql通道
      dataSourceStore.withChannel(id) match {
        case Nil =>
          //校验数据通道是否被数据建模绑定
          modelingStore.byChannel(id) match {
            case Nil =>
              store.byDatasource(id) match {
                case Nil =>
                  val timers = taskStore.byConf(id)
                  val result: BuildResult = if (timers.nonEmpty) {
                    val names = timers.map(_.name).mkString(",")
                    BuildResult("304", "729", name, names)
                    //Result("304", message = s"DELETE channel[$id] FAILED,SOME timer is use it")
                  } else {
                    store.get(id) match {
                      case Some(info) =>
                        if (isStopped(id)) {
                          store.delete(id)
                          BuildResult("200", "704", info.name)
                          //Result("200", s"delete channel[$id] success")
                        } else {
                          BuildResult("304", "721", info.name)
                          //Result("304", s"delete channel[$id] ignore,please stop it first")
                        }
                      case _ =>
                        BuildResult(StatusCodes.NotFound.intValue.toString, "711")
                      //Result(StatusCodes.NotFound.intValue.toString, StatusCodes.NotFound.reason + s"channel[$id] not found")
                    }
                  }

                  if (notBroadcast)
                    sender() ! result
                case data: List[ConfigWrapper@unchecked] =>
                  val names = data.map(_.name).mkString(",")
                  if (notBroadcast)
                    sender() ! BuildResult("304", "735", name, names)
              }
            case list: List[ModelingWrapper] =>
              sender() ! BuildResult("304", "737", name, list.map(modelingWrapper => modelingWrapper.name).mkString(","))
          }
        case list: List[DataSourceWrapper] =>
          sender() ! BuildResult("304", "736", name, list.map(dsWrapper => dsWrapper.name).mkString(","))
      }


    case obj =>
      super.receive(obj)
  }

  def start(id: String, info: ConfigWrapper): Unit = {
    logger.debug(s"start:" + info.id)
    val config = toConfig(info)
println(config)
    context.actorSelection("/user/server").forward((id, (config, Opt.START)))

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

  def channelStatus(id: String): (Status.Status, String) = {
    statusStore.status(id) match {
      case items =>
        val monitorStatus: (Status.Status, Option[String]) = items.filter(_.model == ProcessModel.MONITOR) match {
          case Nil =>
            (Status.STOPPED, None)
          case head :: Nil =>
            (head.value, head.errorMsg)
          case _ =>
            (Status.UNKNOWN, None)
        }
        val streams = items.filter(_.model == ProcessModel.STREAM).map(_.value.toString)
        val streamStatus = if (streams.isEmpty) {
          Status.STOPPED.toString
        } else {
          streams.reduce[String] {
            case (a, b) =>
              if (a.contains("ERROR") || b.contains("ERROR")) {
                "READING_ERROR"
              } else {
                a
              }


          }
        }
        val writers = items.filter(_.model == ProcessModel.WRITER).map(writer => (writer.value.toString, writer.errorMsg.getOrElse("")))

        val (writerStatus, writerMsg) = if (writers.isEmpty) {
          (Status.STOPPED.toString, "")
        } else {
          writers.reduce[(String, String)] {
            case (a, b) =>
              if (a._1.contains("ERROR") || b._1.contains("ERROR")) {
                ("WRITER_ERROR", a._2 + b._2)
              } else {
                a
              }
          }
        }
        val lexers = items.filter(_.model == ProcessModel.LEXER).map(lexer => (lexer.value.toString, lexer.errorMsg.getOrElse("")))
        val (lexerStatus, msg) = if (lexers.isEmpty) {
          (Status.STOPPED.toString, "")
        } else {
          lexers.reduce[(String, String)] {
            case (a, b) =>
              if (a._1.contains("ERROR") || b._1.contains("ERROR")) {
                ("LEXER_ERROR", a._2 + b._2)
              } else {
                a
              }
          }
        }
        if (monitorStatus._1 == Status.RUNNING) {
          val regex = "Exception".r
          logger.info("MonitorStatus = " + monitorStatus._2.toString)
          val status = monitorStatus._2
          if (regex.findFirstIn(status.toString).isDefined) {
            logger.info("MonitorStatus._2 is " + monitorStatus._2.toString)
            return (Status.RUNNING, "文件/目录还不存在")
          }
          //if(monitorStatus._2)
          if (streamStatus.contains("ERROR")) {
            (Status.READER_ERROR, null)
          } else if (writerStatus.contains("ERROR")) {
            (Status.WRITER_ERROR, writerMsg)
          } else if (lexerStatus.contains("ERROR")) {
            (Status.WRITER_ERROR, msg)
          } else {
            (monitorStatus._1, null)
          }
        } else {
          if (monitorStatus._2.isEmpty)
            (monitorStatus._1, null)
          else if (monitorStatus._2.getOrElse("").trim.length > 0) {
            //(monitorStatus._1, "MONITOR_ERROR")
            (monitorStatus._1, monitorStatus._2.getOrElse("MONITOR ERROR"))

          } else
            (monitorStatus._1, null)
        }
    }
  }


  def setStatus(wrapper: ConfigWrapper)(implicit cStatus: Map[String, Status.Status]): ConfigWrapper = {
    wrapper.collector match {
      case Some(id) =>
        cStatus.getOrElse(id, Status.WORKER_STOPPED) match {
          case Status.RUNNING =>
            val stat = channelStatus(wrapper.id)
            logger.debug(s"channel[${wrapper.id}] is " + stat.toString())
            if (stat._1 == Status.STOPPING) {
              wrapper.copy(status = stat._1, errorMsg = null)
            } else {
              wrapper.copy(status = stat._1, errorMsg = Some(stat._2))
            }
          //            wrapper.copy(status = stat._1, errorMsg = Some(stat._2))
          case status =>
            wrapper.copy(status = status)
        }
      case _ =>
        wrapper
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

  def isStopped(confId: String): Boolean = {

    statusStore.monitorStatus(confId) match {
      case None =>
        true
      case Some(Status.STOPPED) =>
        true
      case _ => false

    }
  }

  def operate(operation: Opt.Opt, id: String)(worker: Option[ActorRef]): Unit = {
    var serverInfo: Collector = null
    try {
      // search ConfigInfo info from DB
      store.get(id) match {
        case None =>
          sender() ! BuildResult(StatusCodes.BadRequest.intValue.toString, "730", id)
        //sender() ! Result(StatusCodes.BadRequest.intValue.toString, s"${StatusCodes.BadRequest.reason} : Config[${id}] NOT EXIST")
        case Some(info) =>
          val config = toConfig(info)
          /*启动数据数据通道数据分析类型前，删除上次运行绑定的flink任务id*/
          if (isStopped(config.id) && operation == START && config.channel.`type`.getClass == classOf[AnalyzerChannel]) {
            store.get(config.id) match {
              case Some(wrapper) =>
                self ! (Opt.UPDATE, wrapper.copy(`type` = wrapper.`type`.map(_.asInstanceOf[AnalyzerChannel].copy(jobId = ""))))
              case None =>
                logger.warn(s"no config for configId:$config.id")
            }
          }

          if (isStopped(config.id) && operation == START && checkKnowledgeStatus(config) != null && checkKnowledgeStatus(config).trim.length != 0) {
            //验证解析规则中的知识库的运行状态
            logger.debug(checkKnowledgeStatus(config))
            sender() ! BuildResult(StatusCodes.BadRequest.intValue.toString, "734", id, checkKnowledgeStatus(config))
          } else if ((isStopped(config.id) && operation == START) || (!isStopped(config.id) && operation == STOP)) {
            logger.info(s" $operation config[${config.id}] with remote[${config.collector}]")
            config.collector match {
              case Some(collector) =>
                collectorStore.get(collector) match {
                  case Some(c) =>
                    serverInfo = c
                    worker match {
                      case Some(remote) =>
                        remote.forward((config, operation))
                      case None =>

                        val remoteActor = context.actorSelection("/user/server")
                        logger.info(s"remote $operation config[$config]")
                        // remoteActor.forward((config, operation))
                        val actor = sender()
                        (remoteActor ? (serverInfo.id, (config, operation))).asInstanceOf[Future[BuildResult]].onComplete {
                          case Success(data) =>
                            data.status match {
                              case "304" =>
                                statusServer ! RunStatus(config.id, None, ProcessModel.MONITOR, Status.STOPPED)
                              case _ =>
                            }
                            actor ! data
                          case Failure(e) =>
                            val time = timeout.toString
                            val hostname = Constants.CONF.getString("akka.remote.netty.tcp.hostname")
                            //actor ! Result(StatusCodes.NotModified.intValue.toString, s"Connect to work[$hostname] has timed out[$time], please try again later.")
                            actor ! BuildResult(StatusCodes.NotModified.intValue.toString, "915", hostname, time)
                        }
                    }

                  case None =>
                    sender() ! BuildResult(StatusCodes.NotModified.intValue.toString, "727", StatusCodes.NotModified.reason, id, id)
                  //sender() ! Result(StatusCodes.NotModified.intValue.toString, s"${StatusCodes.NotModified.reason} :The config[${id}] configured failed(collector[${id}] not configured) last time.Please config it again.")
                }
              case None =>
                sender() ! BuildResult(StatusCodes.NotModified.intValue.toString, "727", StatusCodes.NotModified.reason, id, id)
              //sender() ! Result(StatusCodes.NotModified.intValue.toString, s"${StatusCodes.NotModified.reason} :The config[${id}] configured failed(collector[${id}] not configured) last time.Please config it again.")
            }

          }


      }


    } catch {
      case ex: TimeoutException =>
        logger.error("OPERATION ERROR", ex)
        BuildResult(StatusCodes.RequestTimeout.intValue.toString, "807", serverInfo.id + "/" + serverInfo.host + ":" + serverInfo.port)
      //sender() ! Result(StatusCodes.RequestTimeout.intValue.toString, s"${StatusCodes.RequestTimeout.reason} :The collector[${serverInfo.id}/(${serverInfo.host}:${serverInfo.port})] request timeout")
      case ex: Exception =>
        logger.error("OPERATION ERROR", ex)
        sender() ! BuildResult(StatusCodes.InternalServerError.intValue.toString, "902", ex.getMessage)
      //sender() ! Result(StatusCodes.InternalServerError.intValue.toString, s"API SERVICE ERROR : ${ex.getMessage}")
    }
  }

  //校验解析规则中的知识库的运行状态
  final def checkKnowledgeStatus(config: Config): String = {
    val names = new StringBuffer()
    config.channel match {
      case SingleChannel(_, Some(p), _, _, _) =>
        if (p.filter != null)
          p.filter.filter(rule => {
            rule.isInstanceOf[ByKnowledge] && (knowledgeStore.get(rule.asInstanceOf[ByKnowledge].id).isEmpty ||
              !knowledgeStore.get(rule.asInstanceOf[ByKnowledge].id).exists(_.status.contains("FINISHED")))
          }).foreach(knowledgeRule => {
            val byKnowledge = knowledgeRule.asInstanceOf[ByKnowledge]
            names.append(knowledgeStore.get(byKnowledge.id) match {
              case Some(k) => k.name.getOrElse("")
              case None => byKnowledge.name
            }).append("|")
          })

    }
    names.toString
  }


}
