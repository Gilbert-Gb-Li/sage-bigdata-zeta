package com.haima.sage.bigdata.etl.server.worker

import java.util.{TimeZone, UUID}

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model.{Analyzer, AnalyzerModel, Status, writer, _}
import com.haima.sage.bigdata.etl.common.plugin.Checkable
import com.haima.sage.bigdata.etl.driver.{CheckableServer, DriverMate, FlinkMate}
import com.haima.sage.bigdata.etl.knowledge.Knowledge
import com.haima.sage.bigdata.etl.preview.PreviewServer
import com.haima.sage.bigdata.etl.processor.Processor
import com.haima.sage.bigdata.etl.server.knowledge.KnowledgeProcessorServer
import com.haima.sage.bigdata.etl.utils.{Dictionary, DictionaryTable}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

//class Server(path: String) extends Actor {
class Server(override val paths: Set[ActorPath]) extends WithRemote with CheckableServer {
  /**
    * 服务名称
    **/
  override val serverName: String = "/user/server"
  private lazy val logger = LoggerFactory.getLogger(classOf[Server])
  private lazy val watcher = context.actorSelection("/user/watcher")
  private lazy val position = context.actorSelection("/user/position")
  //  private lazy val metric = context.actorOf(Props(classOf[MetricWatcher]).withDispatcher("akka.dispatcher.server"))
  private lazy val metric = context.actorSelection("/user/metric")
  //private[server] val store: ConfigStore = Stores.configStore


  private val configs = new mutable.HashMap[String, (Config, ActorRef)]()
  val knowledges = new mutable.HashMap[String, (Knowledge, ActorRef)]()
  val modelings = new mutable.HashMap[String, (ModelingConfig, ActorRef)]()
  val stopWhenFinishConfigs = new mutable.HashMap[String, Boolean]()
  implicit val timeout = Timeout(10 seconds)
  implicit val context_t = context
  var localClockOffset: Double = 0 //本地时间与master的时间偏移量，单位秒值
  TimeZone.setDefault(TimeZone.getTimeZone("GMT+8")) //设置jvm的时区为东八区
  var originateTimestamp = System.currentTimeMillis()
  logger.info("local server is started!")


  lazy val useDaemon: Boolean = Try(CONF.getBoolean("worker.daemon.enable")).getOrElse(false)
  private var deadLetters: Map[String, Long] = Map()
  private final val zhis: Collector = {
    val host = CONF.getString("akka.remote.netty.tcp.hostname")
    val port = CONF.getInt("akka.remote.netty.tcp.port")
    val id = CONF.getString(ID)
    Collector(id,
      host,
      port, self.path.address.system)
  }

  lazy val daemonService: String = {
    val host = CONF.getString("worker.daemon.host")
    val port = CONF.getInt("worker.daemon.port")
    s"akka.tcp://daemon@$host:$port/user/server"
  }


  def identifyDaemon(): Unit = {
    logger.debug("Identify to Daemon")
    context.actorSelection(daemonService) ! Identify("daemon")
  }


  override def preStart(): Unit = {
    if (useDaemon) {
      identifyDaemon()
    } else {
      identify()
    }


  }


  private val report_remote: ActorRef => Unit = if (!useDaemon) {
    actor: ActorRef => {
      logger.info("server connect running")

      // context.actorSelection("/user/watcher") ! ("server", actor)
      val files = "reader" :: "monitor" :: "writer" :: "protocol" :: "previewer" :: Nil
      val config = files.filter(name => CONF.hasPath(s"app.$name")).map {
        name =>
          (name, CONF.getConfig("app." + name).root().unwrapped())
      }.toMap

      if (!connectWithCluster) {
        logger.info(s"  connected with ${paths.head.toString}")
        logger.info(s"  relay server ${Collectors.from(paths.head.toString)}")

        send((zhis.copy(relayServer = Collectors.from(paths.head.toString)), config))


      } else {
        logger.info(s"  report self to ${paths.mkString("[", ",", "]")}")


        send((zhis, config))
        actor ! (zhis, config)
      }
      context.actorSelection("/user/watcher") forward Opt.RESTART
      if (connectWithCluster) {
        context.actorSelection("/user/relay-server") forward Opt.RESTART
      }

    }
  } else {
    _: ActorRef => {
      context.actorSelection("/user/watcher") forward Opt.RESTART
      if (connectWithCluster) {
        context.actorSelection("/user/relay-server") forward Opt.RESTART
      }

    }
  }

  private def reportWithDaemon(actor: ActorRef): Unit = {
    context.actorSelection("/user/watcher") ! ActorIdentity("server", Some(actor))
    val files = "reader" :: "monitor" :: "writer" :: "protocol" :: "previewer" :: Nil
    val config = files.filter(name => CONF.hasPath(s"app.$name")).map {
      name =>
        (name, CONF.getConfig("app." + name).root().unwrapped())
    }.toMap
    if (!connectWithCluster) {


      send((zhis.copy(relayServer = Collectors.from(paths.head.toString)), config))
    } else {
      send((zhis, config))
    }
    logger.info(s" use daemon[${actor.path}] connected with ${paths.mkString("[", ",", "]")}")
    send((zhis, daemonService, config))

  }

  final def onConnect(actor: ActorRef): Unit = report_remote(actor)

  lazy val _withRemote: ActorRef => Receive = if (useDaemon) {
    identifyDaemon()
    remote: ActorRef => {
      case ("daemon", "received") =>
        logger.debug(s"$zhis add to daemon[${sender().path.toString}]from [$daemonService]")

      case ActorIdentity(_, Some(actor)) =>
        context.watch(actor)
        reportWithDaemon(remote)
        context.unbecome()
        context.become(withDaemon(actor, remote))
      case ActorIdentity(_, None) =>
        //表示本地Server端启动,Restful_Server端未启动
        logger.warn(s"remote[$paths] not available")
        //每隔1秒:请求Restful Server是否开启
        context.system.scheduler.scheduleOnce(10 seconds) {
          identifyDaemon()
        }

    }
  } else {

    remote: ActorRef => {
      context.unbecome()
      withDaemon(null, remote)
    }

  }

  override def withRemote(remote: ActorRef): Receive = _withRemote(remote)

  def retry(): Unit = {
    if (useDaemon) {
      context.unbecome()

    }
    context.unbecome()
    identify()
  }

  def withDaemon(daemon: ActorRef, master: ActorRef): Receive = {

    case r: BuildResult =>
      logger.debug(s"ignore rt $r")

    case Terminated(actor) if actor == daemon =>

      logger.warn(s"daemon:$actor is Terminated")
      context.unwatch(actor)
      retry()
    case letter: DeadLetter =>

      val key = letter.message.toString
      val v = deadLetters.getOrElse(key, 0l)
      if (v % 1000 == 0) {
        val msg = letter.message.toString
        if (msg.length > 100) {
          logger.warn(s"from [${letter.sender.path}] to [${letter.recipient.path}] dead letter [${msg.substring(0, 99)}] times[$v])")
        } else {
          logger.warn(s"from [${letter.sender.path}] to [${letter.recipient.path}] dead letter [$msg] times[$v])")
        }
      }
      deadLetters = deadLetters + (letter.message.toString -> (v + 1l))

    //NTP 时间同步
    case "NTP" =>
      originateTimestamp = System.currentTimeMillis()
      sender() ! (Opt.GET, "NTP")
    case ("NTP", receiveTimestamp: Long, transmitTimestamp: Long, masterLockOffset: Double) =>
      val destinationTimestamp = System.currentTimeMillis()
      localClockOffset = ((receiveTimestamp - originateTimestamp) + (transmitTimestamp - destinationTimestamp)) / 2 + masterLockOffset
      val ntpClock = System.currentTimeMillis() - localClockOffset.toLong
    //      logger.info("Master Server Clock：" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ntpClock))
    case (Opt.LOAD, id: String, current: Int) =>
      forward((Opt.LOAD, id, current))

    case (Opt.LOAD, id: String, Status.FINISHED) =>
      forward((Opt.LOAD, id, Status.FINISHED))
    //知识库启动
    case (datasource: DataSource, parser: Parser[MapRule]@unchecked, path: ActorPath, knowledge: Knowledge) =>
      //如果knowledge存在并且没有start，则start
      sender() ! startKnowledge(datasource, parser, knowledge, path)

    //知识库停止
    case (knowledge: Knowledge, STOP) =>
      logger.debug("stop:" + knowledge)
      knowledges.get(knowledge.id) match {
        case Some(proc) =>
          //如果knowledge存在并且状态不是停止，就停止掉knowledge
          sender() ! stopKnowledge(proc._2)(knowledge)
        case _ =>
          watcher ! (knowledge.id, ProcessModel.KNOWLEDGE, Status.STOPPED)
          sender() ! buildResult("304", s"knowledge[${knowledge.id}] don't running ,please check you all knowledge!", "522", List(knowledge.name.getOrElse(knowledge.id)))
      }
    case (id: String, Opt.CLOSE) =>
      logger.debug(s"clear config[$id] cached")
      if (configs.contains(id)) configs.remove(id)
      if (modelings.contains(id)) modelings.remove(id)
    case (id: String, Opt.RESET) =>
      position ! (Opt.RESET, "%" + id + "%")
      metric ! (Opt.RESET, id)
    case (config: Config, STOP) =>
      logger.debug("stop:" + config)
      configs.get(config.id) match {
        case Some(proc) =>
          //如果config存在并且状态不是停止，就停止掉config
          sender() ! stop(proc._2)(config) //existAndOpt(config, need = true, )
        case _ =>
          sender() ! buildResult("304", s"config[${config.id}] don't running ,please check you all config!", "722", List(config.id))
        //sender() ! buildResult("304", s"config[${config.id}] don't running ,please check you all config!")
      }
    case (config: Config, STOP_WHEN_FINISH) =>
      configs.get(config.id) match {
        case Some(proc) =>
          proc._2 ! STOP_WHEN_FINISH
          sender() ! buildResult("200", s"config[${config.id}] will be stop when all read is finished, please check its status soon!", "723", List(config.id))
        //sender() ! buildResult("200", s"config[${config.id}] will be stop when all read is finished, please check its status soon!")
        case _ =>
          sender() ! buildResult("304", s"config[${config.id}] don't running ,please check you all config!", "722", List(config.id))
          //sender() ! buildResult("304", s"config[${config.id}] don't running ,please check you all config!")
          self ! (config, STOP_WHEN_FINISH)
      }

    case (config: Config, START) =>
      logger.debug("start:" + config)
      configs.get(config.id) match {
        case Some(_) =>
          sender() ! buildResult("304", s"config[${config.id}] is starting!", "713", List(config.id))
        //sender() ! buildResult("304", s"config[${config.id}] is starting!")
        case _ =>
          configs.put(config.id, (config, null))
          //如果config存在并且没有start，则start
          sender() ! start(config)

      }
    case (modelingConfig: ModelingConfig, opt: Opt) =>
      logger.debug(s"receive modeling operation config[${modelingConfig.id}], opt[$opt]")

      def modeling: ActorRef = modelings.get(modelingConfig.id) match {
        case Some(data) =>
          data._2
        case None =>
          val modelingRef = context.actorOf(Props.create(Class.forName(Constants.CONF.getString(Constants.MODELING_PROCESSOR_CLASS)), modelingConfig).withDispatcher("akka.dispatcher.processor"), s"processor-${modelingConfig.id}")
          modelings.put(modelingConfig.id, (modelingConfig, modelingRef))
          modelingRef
      }

      opt match {
        case Opt.START => // 启动操作验证是否安装插件
          watcher ! (modelingConfig, ProcessModel.MONITOR, Status.STARTING)
          val usable = modelingUsable(modelingConfig)
          val result = if (usable.usable) {
            modeling ! opt
            buildResult("200", s"Modeling[${modelingConfig.id}] now  be starting, please check its status soon!", "1110", List(modelingConfig.id, opt.toString))

          } else {
            buildResult("401", s"config[${modelingConfig.id}]  has some error;", "1104", List(modelingConfig.id, usable.cause))
          }
          if (!"200".equals(result.status)) {
            watcher ! (modelingConfig, ProcessModel.MONITOR, Status.STOPPED)
          }
          /*else {
                     watcher ! (modelingConfig, ProcessModel.MONITOR, Status.RUNNING)
                   }*/
          sender() ! result
        case Opt.STOP =>
          watcher ! (modelingConfig, ProcessModel.MONITOR, Status.STOPPING)
          modeling ! (opt, "")
          sender() ! BuildResult("200", "1110", modelingConfig.id, opt.toString)
        case Opt.GET =>
          modeling ! opt
          sender() ! BuildResult("200", "1110", modelingConfig.id, opt.toString)
        case _ =>

          sender() ! BuildResult("404", "1108", opt.toString)
      }
    case ("modeling", configId: String, jobId: String) =>
      forward(("modeling", configId, jobId))
    case ("analyzer", configId: String, jobId: String) =>
      forward(("analyzer", configId, jobId))
    case Opt.CHECK =>
      sender() ! Status.RUNNING
    case (Opt.PREVIEW, msg) =>
      context.actorOf(Props.create(classOf[PreviewServer]), s"preview-server-${UUID.randomUUID()}") forward msg
    case (Opt.CHECK, "flink", url: String) =>
      try {
        val _usable = checkup(FlinkMate(url, AnalyzerModel.MODELING, "flink"))
        if (_usable.usable) {
          sender() ! BuildResult("200", "929", s"[$url]")
        } else {
          sender() ! BuildResult("401", "930", s"${_usable.cause}")
        }
      } catch {
        case e: Exception =>
          logger.error("Check flink url error", e)
          sender() ! BuildResult("500", "901", e.getMessage)
      }

    case ("dictionary", data: Map[String@unchecked, Map[String, (String, String)]@unchecked]) =>
      data.foreach {
        case (key, value) =>
          DictionaryTable.dictionary.+=(key -> value)
      }
    case ("dictionary", key: String, data: Map[String@unchecked, (String, String)@unchecked]) =>
      logger.info("key:" + key + ", data:" + data)
      DictionaryTable.dictionary.+=(key -> data)
    case ("asset", data: Map[String@unchecked, Map[String, String]@unchecked]) =>
      data.foreach {
        case (key, value) =>
          DictionaryTable.asset.+=(key -> value)
      }

    case ("asset", key: String, data: Map[String@unchecked, String@unchecked]) =>
      DictionaryTable.asset.+=(key -> data)
    case dictionary: Dictionary =>
      DictionaryTable.asset = dictionary.asset
      DictionaryTable.fields = dictionary.fields
      DictionaryTable.person = dictionary.person
      DictionaryTable.rules = dictionary.rules
      DictionaryTable.location = dictionary.location

    case query: MetricQuery =>
      metric forward query
    case STOP =>
      configs.foreach(data => stop(data._2._2)(data._2._1))

      send((Opt.KILL, zhis.id))
      val s = sender()
      context.system.scheduler.schedule(1 seconds, 1 seconds) {
        if (configs.isEmpty)
          s ! true
      }

      logger.debug(s" remote[$paths],local stopped!")


    case mate: DriverMate =>
      checkup(mate)
    /*检查数据源是否可用*/
    case ("usable", config: Config) =>
      logger.debug(s"usability check:${config.id}")
      sender() ! usability(config)

    //    case msg =>
    //      logger.warn(s" ignore  message:[ $msg ] sender is:[ ${sender().path} ]")

  }


  def usability(writers: List[Writer]): Usability = {
    checkup(writers.filter(_.isInstanceOf[Checkable]).map(_.asInstanceOf[Checkable]))
  }

  /**
    * 校验字段列表和mate data是否有不匹配字段
    *
    * @param contentType writer.ContentType
    * @param parser      解析器对象
    * @return Usability
    */
  def parserUsable(contentType: Option[writer.ContentType], parser: Parser[MapRule]): Usability = {
    contentType match {
      case Some(delimit: writer.Delimit) =>
        val fieldList = delimit.fields.get.split(delimit.delimit match { case None => "," case Some(d) => d })
        val metadataList = parser match {
          case delimit: Delimit => delimit.fields.toList
          case _ => parser.metadata match {
            case None => List[String]()
            case Some(metadata) => for (tuple <- metadata) yield tuple._1
          }
        }
        var flag = false
        //是否有不匹配的字段，默认是没有  false
        val notMatchFieldsBuf = new StringBuffer()
        fieldList.map(field =>
          if (!metadataList.contains(field)) {
            flag = true
            notMatchFieldsBuf.append(field).append(" ")
          }
        )
        if (flag)
          Usability(usable = false, cause = s"${notMatchFieldsBuf.toString} is not in metaList of configuration parsing rules ")
        else
          Usability()
      case _ =>
        Usability()
    }
  }


  def usability(dataSource: DataSource, writers: List[Writer], parser: Parser[_]): Usability = {
    dataSource match {
      case mate: DriverMate =>
        val sourceUsability = checkup(mate)
        if (sourceUsability.usable) {
          //val list = usability(writers, parser) //先进行字段校验
          //if (list.isEmpty) {
          usability(writers)
          /*} else {
            list
          }*/
        } else {
          sourceUsability
        }
      case _ =>
        //val list = usability(writers, parser)
        /* val list = usability(writers, parser)
         if (list.isEmpty) {*/
        usability(writers)
      /* } else {
         list
       }*/
    }
  }

  def usability(config: Config): Usability = {

    val usableForChannel = usability(config.channel, AnalyzerModel.STREAMING)

    if (usableForChannel.usable) {
      usability(config.writers)
    } else usableForChannel

    /*config.channel match {
      case SingleChannel(mate: Checkable, parser, _, _) if mate != null =>
        val dUsable = checkup(mate)
        if (!dUsable.usable)
          dUsable
        else {
          parser match {
            case Some(mate: Analyzer) =>
              val _usable = checkup(config.channel.`type`.map(_.asInstanceOf[AnalyzerChannel].toMate.copy(model = AnalyzerModel.STREAMING, name = mate.name)).get)
              if (_usable.usable) {
                usability(config.writers)
              } else {
                _usable
              }

            case _ =>
              usability(config.writers)
          }
        }
      case SingleChannel(_, mate: Checkable, _, _) if mate != null =>
        val _usable = checkup(mate)
        if (_usable.usable) {
          usability(config.writers)
        } else {
          _usable
        }
      case _ =>
        Usability(usable = false, cause = s"config[${config.id}] datasource not exist!".toString)

    }*/
  }

  /**
    * Modeling checker
    *
    * @param modeling
    * @return
    */
  def modelingUsable(modeling: ModelingConfig): Usability = {

    val usableOfChannel = usability(modeling.channel, AnalyzerModel.MODELING)
    /*{
      modeling.channel match {
        case SingleChannel(mate1: Checkable, Some(mate2: Checkable), _, _) if mate1 != null =>
          val dUsable1 = checkup(mate1)
          if (dUsable1.usable) {
            val dUsable2 = checkup(mate2)
            if (dUsable2.usable) {
              usability(modeling.sinks)
            } else {
              dUsable2
            }
          } else {
            dUsable1
          }
        case SingleChannel(mate: Checkable, _, _, _) if mate != null =>
          val dUsable = checkup(mate)
          if (dUsable.usable) {
            usability(modeling.sinks)
          } else {
            dUsable
          }
        case _ =>
          usability(modeling.sinks)
      }
    }*/
    if (usableOfChannel.usable) {
      val usableOfParser = modeling.channel.parser match {
        case Some(mate: Analyzer) =>
          checkup(modeling.channel.`type`.map(_.asInstanceOf[AnalyzerChannel].toMate.copy(name = mate.name)).get)
        case _ =>
          Usability()
      }
      if (usableOfChannel.usable) {
        usability(modeling.sinks)
      } else usableOfParser
    } else usableOfChannel

  }

  /**
    * 知识库校验
    *
    * @param kid
    * @return
    */
  def usability(kid: String, dataSource: DataSource): Usability = {
    dataSource match {
      case mate: Checkable =>

        val sourceUsability = checkup(mate)
        if (!sourceUsability.usable) {
          sourceUsability
        } else {
          Usability()
        }
      case _ =>
        Usability(usable = false, cause = s"knowledge[$kid] datasource not exit!".toString)
    }
  }

  /**
    * 检查 Channel 是否可用
    *
    * @param channel Channel
    * @return
    */
  def usability(channel: Channel, model: AnalyzerModel.AnalyzerModel): Usability = {
    channel match {
      case SingleChannel(dataSource: DataSource, parser, _,_, _type) =>
        val usabilityForDatasource = dataSource match {
          case mate: Checkable =>
            checkup(mate)
          case c: Channel =>
            usability(c, model)
          case _ =>
            Usability()
        }
        if (usabilityForDatasource.usable) {
          parser match {
            case Some(mate: Analyzer) =>
              checkup(_type.map(_.asInstanceOf[AnalyzerChannel].toMate.copy(name = mate.name)).get)
            case Some(mate: Checkable) =>
              if (model == AnalyzerModel.STREAMING) checkup(mate)
              else Usability()
            case _ =>
              Usability()
          }
        } else usabilityForDatasource
      case SingleTableChannel(channel: Channel, _, _) =>
        usability(channel, model)
      case TupleChannel(first: Channel, second: Channel, _, _) =>
        val usable = usability(first, model)
        if (usable.usable) {
          usability(second, model)
        } else usable
      case TupleTableChannel(first, second, _) =>
        val usable = usability(first, model)
        if (usable.usable) {
          usability(second, model)
        } else usable
    }
  }

  /**
    * 启动知识库
    *
    * @param dataSource
    * @param parser
    * @param knowledge
    * @param path
    * @return
    */
  def startKnowledge(dataSource: DataSource, parser: Parser[MapRule], knowledge: Knowledge, path: ActorPath): BuildResult = {
    val kid = knowledge.id
    val knowledgeName = knowledge.name.getOrElse("")
    //添加数据源的校验
    val usable = usability(kid, dataSource)
    if (usable.usable) {
      logger.debug(s"load knowledge[$kid] is starting")
      watcher ! (kid, ProcessModel.KNOWLEDGE, Status.STARTING)
      val knowledgeProcessor = context.actorOf(Props.create(classOf[KnowledgeProcessorServer], kid, dataSource, parser, path))
      knowledgeProcessor ! Opt.START
      //context.watch(knowledgeProcessor)
      knowledges.put(kid, (knowledge, knowledgeProcessor))
      buildResult("200", s"knowledge[$kid] now  be starting, please check its status soon!", "515", List(knowledgeName))
    } else {
      //数据源配置有问题
      if (knowledges.get(kid).nonEmpty)
        knowledges.remove(kid)
      buildResult("401", usable.cause, "519", List(knowledgeName, usable.cause))
    }
  }

  /**
    * 停止知识库
    *
    * @param actor     knowledgeProcessor的ActorRef
    * @param knowledge 知识库model
    * @return BuildResult
    */
  def stopKnowledge(actor: ActorRef)(knowledge: Knowledge): BuildResult = {
    watcher ! (knowledge.id, ProcessModel.KNOWLEDGE, Status.STOPPING)
    logger.info(s"stopping knowledge[${knowledge.id}}]")
    actor ! STOP
    watcher ! (knowledge.id, ProcessModel.KNOWLEDGE, Status.STOPPED)
    buildResult("200", s"knowledge[${knowledge.id}] now  be stopping, please check its status soon!", "524", List(knowledge.name.getOrElse(knowledge.id)))
  }

  /**
    * 启动数据通道
    *
    * @param config
    * @return
    */
  def start(config: Config): BuildResult = {
    val usable = usability(config)
    if (usable.usable) {
      watcher ! (config, ProcessModel.MONITOR, Status.STARTING)
      logger.debug(s"start : config[${config.id}]")
      val processor = context.actorOf(Props.create(classOf[Processor], config).withDispatcher("akka.dispatcher.processor"), s"processor-${config.id}")
      processor ! START
      //TODO why ddd watcher ! (config, Status.RUNNING)
      configs.put(config.id, (config, processor))
      buildResult("200", s"config[${config.id}] now  be starting, please check its status soon!", "714", List(config.id))
    } else {
      configs.remove(config.id)
      buildResult("401", s"config[${config.id}] has some error;", "731", List(if ("".equals(config.name) || config.name == null) config.id else config.name, usable.cause))
      //buildResult("401", s"config[${config.id}]  has some error;")
    }
  }

  def stop(actor: ActorRef)(config: Config): BuildResult = {
    watcher ! (config, ProcessModel.MONITOR, Status.STOPPING)
    logger.info(s"stopping config[${config.id}}]")
    actor ! STOP
    buildResult("200", s"config[${config.id}] now  be stopping, please check its status soon!", "716", List(config.id))
    //buildResult("200", s"config[${config.id}] now  be stopping, please check its status soon!")
  }

  def localStop(config: Config): BuildResult = {
    configs.get(config.id) match {
      case None =>
        logger.warn(s"config[${config.id}}] is not running!")
        buildResult("304", s"config[${config.id}] is not running!", "717", List(config.id))
      //buildResult("304", s"config[${config.id}] is not running!")
      case Some(proc) =>
        logger.info(s"stopping config[${config.id}}]")
        proc._2 ! STOP
        buildResult("200", s"config[${config.id}] now  be stopping, please check its status soon !", "716", List(config.id))
      //buildResult("200", s"config[${config.id}] now  be stopping, please check its status soon !")
    }
  }


  /**
    * 注意：此处为偏函数
    * 检查config的状态，执行相应的操作
    *
    * @param isStopped config的状态是否停止，true：当前操作为“启动”，false：当前操作为“停止”
    * @param optFunc   操作方法（启动/停止）
    * @param conf      要操作的config
    * @return 操作结果
    */
  def checkStatusAndOpt(isStopped: Boolean, optFunc: (Config) => BuildResult)(conf: Config): Future[BuildResult] = {

    val optName = if (isStopped) "start" else "stop"

    (watcher ? (Opt.GET, conf.id, ProcessModel.MONITOR)).asInstanceOf[Future[Option[Status.Status]]].map {

      case None => buildResult("304", s"some thing error:config[${conf.id}] Status not found, please check  soon  !", "725", List(conf.id))
      //buildResult("304", s"some thing error:config[${conf.id}] Status not found, please check  soon  !")

      case Some(Status.STOPPED) if isStopped => optFunc(conf)

      case Some(Status.STOPPED) => buildResult("304", s"$optName config[${conf.id}}] won't execute, because it's already stopped!", "732", List(optName.toString, conf.id))
      //buildResult("304", s"$optName config[${conf.id}}] won't execute, because it's already stopped!")

      case Some(Status.STOPPING) if isStopped => optFunc(conf)

      case Some(Status.STOPPING) => buildResult("304", s"$optName config[${conf.id}}] won't execute, because it's in stopping!", "733", List(optName.toString, conf.id))
      //buildResult("304", s"$optName config[${conf.id}}] won't execute, because it's in stopping!")

      case _ if !isStopped => optFunc(conf)

      case _ => buildResult("304", s"you want $optName ,config[${conf.id}] must be running, please stop it first!", "726", List(optName.toString, conf.id))
      //buildResult("304", s"you want $optName ,config[${conf.id}] must be running, please stop it first!")

    }
  }

  var onUpdate = false

  /*abstract class Check extends Actor {
    def store: ConfigStore

    def exits(config: Config, need: Boolean)(f: (Config) => BuildResult): BuildResult = {
      f(config)
    }

    def check(opt: String, onStopped: Boolean)(f: (Config) => BuildResult)(conf: Config): Future[BuildResult] = {
      try {
        implicit val timeout = Timeout(10 seconds)
        (watcher ? (Opt.GET, conf.id, ProcessModel.MONITOR)).asInstanceOf[Future[Option[Status.Status]]] map {
          case None =>
            buildResult("404", s"some thing error:config[${conf.id}] Status not found, please check  soon  !", "718", List(conf.id))
          //buildResult("404", s"some thing error:config[${conf.id}] Status not found, please check  soon  !")
          case Some(Status.STOPPED) =>
            if (onStopped) {
              f(conf)
            } else {
              buildResult("304", s"$opt config[${conf.id}}] won't execute, because it's already stopped!", "732", List(opt.toString, conf.id))
              //buildResult("304", s"$opt config[${conf.id}}] won't execute, because it's already stopped!")
            }
          case Some(Status.STOPPING) =>
            if (onStopped) {
              f(conf)
            } else {
              buildResult("304", s"$opt config[${conf.id}}] won't execute, because it's in stopping!", "733", List(opt.toString, conf.id))
              //buildResult("304", s"$opt config[${conf.id}}] won't execute, because it's in stopping!")
            }
          case _ =>
            if (!onStopped) {
              f(conf)
            } else {
              buildResult("304", s"you want $opt config[${conf.id}] must be running please stop it first!", "726", List(opt.toString, conf.id))
              //buildResult("304", s"you want $opt config[${conf.id}] must be running please stop it first!")
            }
        }
      } catch {
        case _: Exception =>
          check(opt, onStopped)(f)(conf)
      }
    }
  }*/

  def buildResult(code: String, msg: String): Result = {

    Result(code, msg)
  }

  def buildResult(status: String, msg: String, key: String, param: List[String]): BuildResult = {
    if (param.lengthCompare(1) == 0) {
      BuildResult(status, key, param.head)
    }
    else if (param.lengthCompare(2) == 0) {
      BuildResult(status, key, param.head, param(1))
    }
    else {
      BuildResult(status, key)
    }
  }


}

