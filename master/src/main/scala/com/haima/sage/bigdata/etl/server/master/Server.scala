package com.haima.sage.bigdata.etl.server.master

import akka.actor.{ActorIdentity, ActorRef, Address, Identify, Props, Terminated}
import akka.pattern.ask
import akka.remote.{AssociatedEvent, AssociationErrorEvent, DisassociatedEvent, QuarantinedEvent}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.{BroadcastServer, ConfigServer, ModelingServer}
import com.haima.sage.bigdata.etl.server.knowledge.StoredKnowledgeUserServer
import com.haima.sage.bigdata.etl.store.{CollectorStore, Stores}
import com.haima.sage.bigdata.etl.utils.NtpUtil

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Success

/**
  * Created by zhhuiyan on 2015/2/2.
  */
class Server extends BroadcastServer {

  import context.dispatcher

  // private final val daemons: mutable.HashMap[String, String] = mutable.HashMap[String, String]()

  lazy val store: CollectorStore = Stores.collectorStore
  private lazy val collector_server = context.actorSelection("/user/collector-server")
  private lazy val configServer = context.actorOf(Props[ConfigServer])
  private lazy val modelingServer = context.actorOf(Props[ModelingServer])
  private lazy val NTP_CYCLE = Constants.MASTER.getString("ntp.server.synchronizing.cycle").toInt.asInstanceOf[Long]
  //  private lazy val licenseChecker = context.actorOf(Props[LicenseChecker])
  private val writerNamesInfo: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
  /*private val monitorNameInfo: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
  private val metricCounterFactory = scala.collection.mutable.HashMap[String, BigDecimal]()*/
  // private val remotes = scala.collection.mutable.HashMap[String, String]()

  private val ACTOR_REF_PATH_SUFFIX = "/user/server"


  case class WorkerActor(id: String,
                         worker: Collector,
                         real: Option[ActorRef] = None,
                         proxy: Option[ActorRef] = None,
                         daemon: Option[String] = None,
                         status: Status.Status = Status.RUNNING) {
    def send(msg: Any): Unit = {
      proxy match {
        case Some(actor) =>
          actor ! (real.get, Opt.SEND, msg)
        case _ =>
          real.foreach(_.!(msg))
      }
    }

    def !(msg: Any) = send(msg)

    def forward(msg: Any): Unit = {
      proxy match {
        case Some(actor) =>
          actor ! (real.get, Opt.FORWARD, msg)
        case _ =>
          real.foreach(_.forward(msg))
      }


    }

    override def hashCode(): Int = id.hashCode + worker.address().hashCode
  }


  override def preStart(): Unit = {
    super.preStart()
    self ! Opt.RESTART
    self ! (Opt.SYNC, "NTP")
  }

  val knowledgeServerCache: mutable.Map[String, ActorRef] = mutable.Map[String, ActorRef]()


  private def getID(id: String): String = {
    if (id.startsWith("KNOWLEDGE_", 0)) id else s"KNOWLEDGE_" + id.split("/").last.replace("-", "_").toUpperCase()
  }

  def active(workers: Set[WorkerActor]): PartialFunction[Any, Unit] = {
    case Opt.RESTART =>
      if (notBroadcast) {

        store.all().foreach(c => {
          if (c.status.contains(Status.RUNNING))
            restart(c)
        })

      }

    case ("modeling", configId: String, jobId: String) =>
      modelingServer ! ("modeling", configId, jobId)
    case ("analyzer", configId: String, jobId: String) =>
      configServer ! (Opt.UPDATE, configId, jobId)


    //worker使用daemon连接master
    case (Opt.SYNC, (worker: Collector, daemon: String, properties: Map[_, _])) =>
      val actor = sender()
      if (notBroadcast) {
        logger.info(s"receive collector info with daemon : ${worker.id}/${worker.host}:${worker.port}")
        //sender() ! Result("200", s"receive collector info with daemon : ${worker.id}/${worker.host}:${worker.port}")
        sender() ! BuildResult("200", "802", worker.id + "/" + worker.host + ":" + worker.port)
        configServer ! (Opt.START, worker.id, actor)
      }
      logger.info(s"receive collector info with daemon : ${worker.id}/${worker.host}:${worker.port}")
      saveInfo(worker, properties)
      context.unbecome()
      context.become(withWorkers(workers + WorkerActor(worker.id,
        worker)))
      worker.relayServer match {
        case Some(x) =>
          identifyingRelayServer(worker)
        case _ =>
          identifying(worker)
      }
      collector_server ! (Opt.UPDATE, worker.copy(status = Some(Status.RUNNING)))
      collector_server ! (Opt.PUT, worker.id, daemon)

    //worker不使用daemon连接master
    case (Opt.SYNC, (worker: Collector, properties: Map[_, _])) =>
      val actor = sender()
      logger.info(s"receive collector info : ${worker.id}/${worker.host}:${worker.port},${worker.relayServer}")
      collector_server ! (Opt.UPDATE, worker.copy(status = Some(Status.RUNNING)))
      saveInfo(worker, properties)
      context.unbecome()
      context.become(withWorkers(workers + WorkerActor(worker.id,
        worker)))
      worker.relayServer match {
        case Some(_) =>
          identifyingRelayServer(worker)
        case _ =>
          identifying(worker)
      }

      if (notBroadcast) {
        sender() ! BuildResult("200", "802", worker.id + "/" + worker.host + ":" + worker.port)
        configServer ! (Opt.START, worker.id, actor)

      }





    //对daemon进行操作
    case (Opt.SYNC, (Opt.DELETE, id: String)) =>
      logger.info(s"remove worker[$id] from cluster.")
      //daemons -= id
      context.unbecome()
      context.become(withWorkers(workers.dropWhile(_.id == id)))
    case (Opt.DELETE, id: String, "daemon") =>
      logger.debug(s"remove daemon with worker[$id]")

      workers.dropWhile(_.id == id)
    //    FIXME case (Opt.SYNC, (Opt.PUT, id: String, daemon: String)) =>
    //      logger.debug(s"add daemon[$daemon] with worker[$id]")
    //      daemons += (id -> daemon)
    //      logger.info(s"daemon list:$daemons")
    case (Opt.START, id: String) =>
      workers.find(w => w.id == id && w.daemon.isDefined) match {
        case Some(x) =>
          context.actorSelection(x.daemon.get).forward((id, Opt.START))
        case _ =>
          sender() ! Status.FAIL
      }

    //      daemons.get(id) match {
    //        case Some(daemon) =>
    //          logger.info(s"Start daemon $id")
    //          context.actorSelection(daemon).forward((id, Opt.START))
    //        case _ =>
    //          sender() ! Status.FAIL
    //        //sender() ! Result(StatusCodes.NotFound.intValue.toString, s"${StatusCodes.NotFound.reason} :daemon for COLLECTOR[$id] NOT FOUND")
    //      }
    /*停止worker 使用脚本*/
    case (Opt.KILL, id: String) =>
      workers.find(w => w.id == id) match {
        case Some(x) =>
          logger.debug(s"shell kill worker $x ")
          collector_server ! (Opt.UPDATE, id, Status.STOPPING)
          if (x.daemon.isDefined)
            context.actorSelection(x.daemon.get) ! (id, Opt.STOP)
          context.unbecome()
          context.become(withWorkers(workers.filter(_.id != id)))
        case _ =>
      }


    // 脚本-停止一个worker 自动停止
    case (Opt.STOP, id: String) =>
      //val data: Collector = store.get(id).get
      val s = sender()

      workers.find(w => w.id == id && w.daemon.isDefined) match {
        case Some(x) =>
          /*
           *  TODO status update
           */ {
          collector_server ! (Opt.UPDATE, id, Status.STOPPING)
          logger.debug(s"to stop worker $x ")
          context.actorSelection(x.daemon.get) ? (id, Opt.STOP)
        }.onComplete {
          //case Success(rt@Result("200", msg)) => {
          case Success(rt@BuildResult("200", "928", msg)) => {
            s ! BuildResult("200", "928", "stop worker server success")
          }
          case _ =>
            logger.info("stop worker server error")
            s ! BuildResult("304", "928", "stop worker server error")
          //sender() ! Result(StatusCodes.NotFound.intValue.toString, s"${StatusCodes.NotFound.reason} :daemon for COLLECTOR[$id] NOT FOUND")

        }
        case _ =>
          s ! Status.FAIL
      }

    case (Opt.SYNC, (Opt.STOP, id: String)) =>
      logger.info(s"Stopping $id")
      if (notBroadcast) {
        val collector = store.get(id).orNull
        sender() ! BuildResult("200", "808", if (collector == null) "" else collector.name)
        //sender() ! Result("200", s"stop worker[$id]")
      }

      collector_server ! (Opt.UPDATE, id, Status.STOPPING)

      context.unbecome()
      context.become(withWorkers(workers.dropWhile(w => w.id == id)))


    case (id: String, msg) =>
      workers.find(w => w.id == id) match {
        case Some(w) =>
          logger.debug(s"forward to worker $msg")
          w.forward(msg)
        case _ =>
          logger.debug(s"workers $workers")
          logger.debug("ignore to " + id)
          val collector = store.get(id).orNull
          sender() ! BuildResult("304", "800", if (collector == null) "" else collector.name)
      }
  }


  override def receive: PartialFunction[Any, Unit] = withWorkers(Set())

  def watch(workers: Set[WorkerActor]): PartialFunction[Any, Unit] = {
    /* 监听到 worker 丢失连接事件*/
    case DisassociatedEvent(local, remote, _) =>
      logger.debug(s"sender[${sender()}] DisassociatedEvent $remote")
      remove(workers, remote)
    /*监听到 worker 连接事件*/
    case AssociatedEvent(local, remote, _) =>

      logger.debug(s"AssociatedEvent : remote = $remote local = $local")
    case Some(worker: Collector) =>
      logger.info(s"Worker($worker) status is " + worker.status)
      if (!worker.status.contains(Status.STOPPING)) {
        collector_server ! (Opt.UPDATE, worker.copy(status = Some(Status.RUNNING)))
        //FIXME identifying(worker)
      }

    case AssociationErrorEvent(cause, _, remote, _, _) =>
      logger.debug(s"AssociationErrorEvent $remote")
      //remove(remote)
      cause.getClass.getName match {
        case "akka.remote.InvalidAssociation" =>
          logger.error(s"AssociationError:The remote system has quarantined this system. No further associations to the remote system are possible until this system is restarted.")
        //task ! (Opt.STOP, s"$remote$ACTOR_REF_PATH_SUFFIX")
        case "akka.remote.EndpointAssociationException" =>
          logger.warn(s"AssociationError:$cause")
        case other: String =>
          logger.warn(s"AssociationError:Unknown AssociationErrorEvent Type : $other")
      }
    /*监听到 worker 不可用事件*/
    case QuarantinedEvent(address, uid) =>
      logger.warn(s"Quarantined [address:$address,uid:$uid]")
    /* 监听到 worker 丢失连接事件*/
    //    case DisassociatedEvent(local, remote, _) =>
    //      logger.debug(s"sender[${sender()}] DisassociatedEvent ${remote}")
    //      remove(remote)
    /*监听到 worker 终止事件*/
    case Terminated(actor) =>

      logger.debug(s"terminated ${actor.path}")
      remove(workers, actor.path.address)


    case msg =>
      super.receive(msg)

  }


  /**
    * ntp 服务
    **/
  def ntp(workers: Set[WorkerActor]): PartialFunction[Any, Unit] = {
    //同步时钟
    case (Opt.SYNC, "NTP") =>

      //将master的时间定时的广播给workers
      val receiveTimestamp = System.currentTimeMillis()
      val clockOffset = if (Constants.MASTER.getBoolean("ntp.server.enable")) NtpUtil.localClockOffset else 0.0
      val transmitTimestamp = System.currentTimeMillis()
      workers.foreach(worker => worker ! ("NTP", receiveTimestamp, transmitTimestamp, clockOffset))
      context.system.scheduler.scheduleOnce(NTP_CYCLE milliseconds) {
        self ! (Opt.SYNC, "NTP")
      }
  }

  /**
    * 知识库-服务
    **/
  val knowledgeService: PartialFunction[Any, Unit] = {
    /*同步存储的知识库数据到 需要使用的地方*/
    case (Opt.LOAD, id: String, current: Int) =>
      val kid = getID(id)
      knowledgeServerCache.getOrElseUpdate(kid, context.actorOf(Props.create(classOf[StoredKnowledgeUserServer], kid), kid)) forward(Opt.SYNC, current)

    //知识库同步完成
    case (Opt.LOAD, id: String, Status.FINISHED) =>
      val kid = getID(id)
      knowledgeServerCache.getOrElseUpdate(kid, context.actorOf(Props.create(classOf[StoredKnowledgeUserServer], kid), kid)) forward(Opt.SYNC, Status.FINISHED)

    //知识库加载完成，通知知识库使用者，更新知识缓存
    case (id: String, "KNOWLEDGE", "LOAD", "FINISHED") =>
      val kid = getID(id)
      knowledgeServerCache.get(kid) match {
        case Some(actor) =>
          actor ! ("KNOWLEDGE", "UPDATE")
        case None =>
          logger.debug("no users!")
      }
  }


  def withWorkers(workers: Set[WorkerActor]): PartialFunction[Any, Unit] = active(workers)
    .orElse(identityService(workers))
    .orElse(knowledgeService).orElse(ntp(workers)).orElse(watch(workers))

  def identityService(workers: Set[WorkerActor]): PartialFunction[Any, Unit] = {
    case ActorIdentity(id: String, None) =>

      context.system.scheduler.scheduleOnce(10 seconds) {
        if (id.startsWith("forward-")) {

          workers.find(_.id == id.substring(8)).map(_.worker).foreach(identifyingRelayServer)
        } else {
          workers.find(_.id == id).map(_.worker).foreach(identifyingRelayServer)
        }
      }

    case ActorIdentity(id: String, Some(actor)) =>

      // FIXME context.watch(actor)
      context.unbecome()
      if (id.startsWith("forward-")) {
        logger.info(s"find relay-server worker[$actor].")
        context.become(withWorkers(workers.map {
          case w if ("forward-" + w.id) == id =>
            actor ! (w.worker, Opt.SEND, Identify(w.id))
            w.copy(proxy = Some(actor))
          case w =>
            w
        }))
      } else {
        logger.info(s"find worker[$actor].")
        context.become(withWorkers(workers.map {
          case w if w.id == id =>
            w.copy(real = Some(actor))
          case w =>
            w
        }))
        if (notBroadcast) {
          configServer ! (Opt.START, id, actor)
        }
      }


  }

  private def saveInfo(collector: Collector, properties: Map[_, _]): Unit = {
    val json = mapper.writeValueAsString(properties.map {
      pi_parent =>
        val initialWriterNames = writerNamesInfo.isEmpty && pi_parent._1.equals("writer")
        (pi_parent._1.toString, pi_parent._2.asInstanceOf[java.util.HashMap[String, Object]].map {
          pi_children =>
            // 获取配置文件中writer类型名称信息
            if (initialWriterNames) writerNamesInfo.+=(pi_children._1)
            (pi_children._1, pi_children._2 match {
              case other: Object =>
                s"$other"
            })
        }.toMap)
    })


    DataSource.monitorNamesInfo.clear()
    DataSource.writerNamesInfo.clear()
    DataSource.previewersInfo.clear()
    JDBCSource.protocolNamesInfo.clear()
    properties.foreach {
      worker => {
        worker._2.asInstanceOf[java.util.HashMap[String, Object]].foreach {
          props => {
            worker._1 match {
              case "writer" => DataSource.writerNamesInfo += props._1
              case "monitor" => DataSource.monitorNamesInfo += props._1
              case "protocol" => JDBCSource.protocolNamesInfo += props._1
              case "previewer" =>
                DataSource.previewersInfo += (props._1 -> collector)
              case _ =>
            }
          }
        } //
      }
    }
    logger.info(s"lexers:${DataSource.previewersInfo.toString()}")
    logger.info(s"""receive writer names info :  $writerNamesInfo""")
  }


  private def identifying(worker: Collector): Unit = {
    logger.debug(s"identifying  worker[${getPath(worker)}].")
    context.actorSelection(getPath(worker)) ! Identify(worker.id)
  }

  private def restart(worker: Collector): Unit = {
    logger.debug(s"restart  worker[${getPath(worker)}].")
    context.actorSelection(getPath(worker)) ! Opt.RESTART
    context.actorSelection(getPath(worker, "/watcher")) ! Opt.RESTART
  }

  private def identifyingRelayServer(worker: Collector): Unit = {
    val path = getPath(worker.relayServer.get, "relay-server")
    logger.debug(s"identifying relayServer worker[$path].")
    context.actorSelection(path) ! Identify("forward-" + worker.id)
  }

  private def getPath(worker: Collector, name: String = "server") = {


    s"akka.tcp://${worker.system}@${worker.host}:${worker.port}/user/${name}"
  }

  def remove(workers: Set[WorkerActor], remote: Address): Unit = {
    logger.info(s"detected unreachable:${remote.toString + ACTOR_REF_PATH_SUFFIX},remote:$remote")

    workers.find(t => {
      t.worker.address().contains(remote.toString)
    }) match {
      case Some(w) =>
        val id = w.id
        val collector = store.get(id)
        logger.info("Worker status is " + collector.get.status + " now.")
        logger.info("Is Running?:" + collector.get.status.contains(Status.RUNNING))
        if (collector.get.status.contains(Status.STOPPING) || collector.get.status.contains(Status.STOPPED)) {
          collector_server ! (Opt.SYNC, (Opt.UPDATE, id, Status.STOPPED))

        } else {
          logger.info("set UNAVAILABLE")
          collector_server ! (Opt.SYNC, (Opt.UPDATE, id, Status.UNAVAILABLE))
        }
        context.unwatch(w.real.get)
        context.unbecome()
        val set = workers.filter(t => {
          !t.worker.address().contains(remote.toString)
        })
        logger.debug(s"workers is ${set.size}")
        context.become(withWorkers(set))
      case None =>
        logger.debug(s" DisassociatedEvent : event.remoteAddress = $remote")
    }
  }


}
