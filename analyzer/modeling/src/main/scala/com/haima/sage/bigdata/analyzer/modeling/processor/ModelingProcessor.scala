package com.haima.sage.bigdata.analyzer.modeling.processor

import java.io.File
import java.net.URL
import java.util.{Collections, Date, UUID}

import akka.actor.{Actor, ActorIdentity, ActorRef, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.analyzer.dag.DAGBuilder
import com.haima.sage.bigdata.analyzer.modeling.DataModelingAnalyzer
import com.haima.sage.bigdata.analyzer.modeling.output.AkkaOutputFormat
import com.haima.sage.bigdata.analyzer.modeling.reader.FlinkModelingReader
import com.haima.sage.bigdata.analyzer.utils.JarUtils
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.{CONF, MONITORS, PROCESS_CACHE_SIZE, PROCESS_CACHE_SIZE_DEFAULT, executor}
import com.haima.sage.bigdata.etl.common.Implicits.richMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.Status.Status
import com.haima.sage.bigdata.etl.common.model.filter.{MapRule, ReParser}
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.driver.CheckableServer
import com.haima.sage.bigdata.etl.lexer.filter.ReParserProcessor
import com.haima.sage.bigdata.etl.metrics.{MeterReport, MetricRegistrySingle}
import com.haima.sage.bigdata.etl.plugin.flink.{FlinkAddress, FlinkStatusProcessor, FlinkWatcher}
import com.haima.sage.bigdata.etl.utils.{AkkaUtils, Mapper, TimeUtils}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.api.common.JobID
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.client.program.rest.RestClusterClient
import org.apache.flink.client.program.{ClusterClient, ProgramInvocationException, StandaloneClusterClient}
import org.apache.flink.configuration.{Configuration, JobManagerOptions}
import org.apache.flink.core.fs.Path
import org.apache.flink.optimizer.costs.DefaultCostEstimator
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator
import org.apache.flink.optimizer.{DataStatistics, Optimizer}
import org.apache.flink.runtime.client.JobCancellationException
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.table.api.BatchTableEnvironment

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
  * Created by evan on 17-8-10.
  */
class ModelingProcessor(conf: ModelingConfig) extends Actor with CheckableServer with FlinkStatusProcessor {
  implicit val timeout = Timeout(1 minutes)

  // 建模通道状态监控
  private lazy val watcher = context.actorSelection("/user/watcher")
  // flink Job 状态监控
  override val flinkWatcher = conf.channel.`type` match {
    case Some(AnalyzerChannel(cluster, _, _, _)) =>
      context.actorOf(Props.create(classOf[FlinkWatcher], cluster), s"flink-watcher-${conf.id}")
    case _ =>
      /*TODO FIXME*/
      throw new NotImplementedError("modeling processor not support none analyze channel")
  }

  // Metrics 单实例
  val metric = MetricRegistrySingle.build()

  // 执行器ActorRef
  var processorExecutor: ActorRef = _

  //写程序错误标示，正常是没有错误
  var fatal = false

  //每次读取条数
  val CACHE_SIZE: Long = CONF.getLong(PROCESS_CACHE_SIZE) match {
    case 0 =>
      PROCESS_CACHE_SIZE_DEFAULT
    case a =>
      a
  }

  @volatile
  private var repeat = 1
  //轮询时间
  //  val POLL_TIME: Long = Try(modelingConfig.get("polling_ms", Constants.CONF.getString(PROCESS_WAIT_TIMES)).toLong).getOrElse(PROCESS_WAIT_TIMES_DEFAULT)
  val POLL_TIME: Long = 2000
  // Writers列表（运行时ID，classname，Writer实例）
  val writes: List[(String, String, Writer)] = conf.sinks.zipWithIndex.map {
    case (wc, index) =>
      (s"writer-${wc.name}-$index@${conf.id}".replaceAll("[/\\\\]", "."), CONF.getConfig("app.writer").getString(wc.name), wc)
  }

  // writes Metrics meter Report（MeterName -> MeterReport）
  val write_meters: Map[String, MeterReport] = writes.map(name => {
    val n = s"${name._1}_write"
    (n, MeterReport(None, None, None))
  }
  ).toMap

  // Writer ActorRef列表
  val writers: List[ActorRef] = writes.map {
    case (name, clazz, wc) =>
      clazz match {
        case msg: String =>
          self ! (name, ProcessModel.WRITER, Status.PENDING)
          //优化 ,当数据输出的缓存大于系统配置的缓存时使用系统缓存作为数据输出缓存,加快数据入库速度(不然要等到数据输出触发刷新时间才会读取下一个缓存)
          context.actorOf(Props.create(Class.forName(msg), wc match {
            case w: ES5Writer =>
              w.copy(cache = if (w.cache > CACHE_SIZE) {
                logger.warn(s"writer.cache[${w.cache}] big then process.cache.size[$CACHE_SIZE],use process.cache.size[$CACHE_SIZE].")
                CACHE_SIZE.toInt
              } else {
                w.cache
              })
            case obj =>
              obj
          }, write_meters(s"${name}_write"))
            .withDispatcher("akka.dispatcher.executor"), name)
      }
  }

  override def receive: Receive = {
    case Opt.START =>
      try {


        if (flinkWatcher != null) {
          // 判断 Flink 连接是否可用
          Await.result(flinkWatcher ? FlinkAPIs.RPC_ADDRESS, timeout.duration).asInstanceOf[Option[String]] match {
            case Some(clusterAddress) =>
                  // 可用资源个数
                  var availableSlots: Int = 0

                  // 判断资源是否可用
                  availableSlots = Await.result(flinkWatcher ? FlinkAPIs.SLOTS_AVAILABLE, timeout.duration).asInstanceOf[Int]
                  if (availableSlots > 0) {

                    val c = conf.channel.dataSource match {
                      case d: Channel =>
                        d
                      /*直接分析时*/
                      case s: DataSource =>
                        conf.channel
                    }

                    processorExecutor = context.actorOf(Props.create(classOf[ModelingExecutor[_]],
                      this, c,
                      conf.channel.parser.map(_.asInstanceOf[Analyzer]).get, clusterAddress.split(":")
                    ).withDispatcher("akka.dispatcher.executor"), s"modeling-executor-${UUID.randomUUID()}")
                  } else {
                    self ! (Opt.STOP, "no flink slots available")
                  }

            case _ =>
              self ! (Opt.STOP, "connect failed")
          }
        }

      }
      catch {
        case e: Exception =>
          logger.error("Create ModelingProcessorExecutor error", e)
          self ! (Opt.STOP, "Create ModelingProcessorExecutor error")
      }
    case Opt.STOP
    =>
      self forward(Opt.STOP, "")
    case (Opt.STOP, message: String)
    =>
      logger.debug(s"Stopping ModelingProcessor. $message")

      watcher ! (conf, ProcessModel.MONITOR, Status.STOPPING, message)

      //      // 停止状态检查轮循
      //      if (watcherCancellable != null && watcherCancellable.cancel()) {
      //        logger.info(s"Cancel watcherCancellable.")
      //        watcherCancellable = null
      //      }
      // 停止执行器
      if (processorExecutor != null) {
        logger.info("Stopping ModelingProcessorExecutor")
        processorExecutor ! (Opt.STOP, message)
      } else {
        logger.info("modeling processor executor is closed , set status STOPPED.")
        // 停止数据存储
        //        logger.debug(s"Stopping writers, writers.size = ${writers.size}")
        // writers.foreach(_ ! Opt.STOP)
        context.children.foreach(_ ! Opt.STOP)
        // flinkWatcher ! Opt.STOP
        import scala.concurrent.duration._
        if ((context != null && context.children != null && context.children.nonEmpty && repeat < 1000) && !fatal) {
          if (repeat % 100 == 1)
            logger.debug(s"${conf.id} waiting[$POLL_TIME ms] for children[${context.children.map(_.path.name).mkString(";")}]")
          repeat += 1
          context.system.scheduler.scheduleOnce(POLL_TIME milliseconds) {
            if (context != null && context.children != null && context.children.nonEmpty)
              self ! (Opt.STOP, message)
            else {
              clear(message)
            }
          }
        } else {
          if (fatal)
            logger.warn(s"Because the writer error, stop ${self.path.name} directly.")
          clear(message)
        }
        /*context.system.scheduler.scheduleOnce(2000 milliseconds) {
          if (context != null && context.children != null && context.children.nonEmpty) {
            //            context.children.foreach(_ ! Opt.STOP)
            self ! (Opt.STOP, message)
          }
          else {
            clear(message)
          }
        }*/
      }
    case (Opt.CLOSE, jobId: String, msg: String)
    =>
      flinkWatcher ! (jobId, FlinkAPIs.JOB_CONCELLATION)
      context.system.scheduler.scheduleOnce(2000 milliseconds) {
        try {
          logger.info(s"Cancelling flink job[$jobId]. ")
          Await.result(flinkWatcher ? (jobId, FlinkAPIs.JOB_STATUS), timeout.duration).asInstanceOf[Status] match {
            case Status.UNAVAILABLE =>
              logger.warn(s"connect to flink fail when closing job[$jobId], try again later.")
              self ! (Opt.CLOSE, jobId, msg)

            case Status.RUNNING =>
              self ! (Opt.CLOSE, jobId, msg)
            case status =>
              logger.info(s"Receive status[$status], when closing job[$jobId]. Start to close executor.")
              processorExecutor ! (Opt.CLOSE, msg)
          }
        } catch {
          case ex: Exception =>
            logger.error("Closing job[$jobId] error", ex)
            self ! (Opt.CLOSE, jobId)
        }
      }

    case ("modeling", configId: String, jobId: String)
    =>
      logger.debug(s"receive modeling flink jobId[$jobId] of config[$configId]")
      context.become(apply(jobId).orElse(receive))

      /*
      * 报告给master 任务的地址
      * */
      context.parent ! ("modeling", configId, jobId)
    //      // 接收到 jobID 后开始启动job监控轮询
    //      if (flinkWatcher != null && jobId != null && !"".equals(jobId)) {
    //        watcherCancellable = context.system.scheduler.schedule(10 seconds, 10 seconds) {
    //          try {
    //            Await.result(flinkWatcher ? (jobId, FlinkAPIs.JOB_STATUS), timeout.duration).asInstanceOf[Status] match {
    //              case Status.FAIL =>
    //                self ! (Opt.STOP, s"Flink job [$jobId] execute fail.")
    //              case Status.FINISHED =>
    //                logger.info(s"Flink job [$jobId] finished.")
    //                self ! (Opt.STOP, "")
    //              case Status.ERROR =>
    //                self ! (Opt.STOP, s"Flink job [$jobId] execute error.")
    //              case Status.UNKNOWN =>
    //                logger.warn(s"Flink job [$jobId] is UNKNOWN, flinkWatcher will check again after 10 seconds.")
    //              //                self ! (Opt.STOP, s"flink job [$jobId] is UNKNOWN")
    //              case Status.STOPPED =>
    //                self ! (Opt.STOP, s"Flink job [$jobId] is cancelled.")
    //              case Status.RUNNING =>
    //                logger.info(s"Flink job [$jobId] is running.")
    //                watcher ! (conf, ProcessModel.MONITOR, Status.RUNNING)
    //              case _ =>
    //
    //            }
    //          } catch {
    //            case ex: Exception =>
    //              logger.error(s"Check the status of flink job[$jobId] error, when executing WatcherCancellable ", ex)
    //          }
    //        }
    //      }

    case (path: String, ProcessModel.WRITER, Status.STOPPED)
    =>
      //所有输出 执行完成
      if (context.children.isEmpty) {
        logger.debug(s" writer[$path] closed")
        watcher ! (conf, ProcessModel.WRITER, path, Status.STOPPED)

      }
    //处理writer的状态信息
    case (path: String, ProcessModel.WRITER, status: Status.Status)
    =>
      watcher ! (conf, ProcessModel.WRITER, path, status)
      status match {
        case Status.ERROR =>
          fatal = true
          self ! (Opt.STOP, s"WRITER ERROR")
        case _ =>
          fatal = false
      }
    //处理writer的状态信息
    case (path: String, ProcessModel.WRITER, status: Status.Status, msg)
    =>
      //writer 错误标示
      watcher ! (conf, ProcessModel.WRITER, path, status, msg)
      status match {
        case Status.RUNNING =>
          fatal = false
        case _ =>
          fatal = true
          self ! (Opt.STOP, s"WRITER ERROR: $msg")
      }
  }

  def clear(message: String): Unit = {
    write_meters.keys.foreach {
      key =>
        metric.remove(s"${key}_success")
        metric.remove(s"${key}_fail")
    }
    // 移除 Worker Server端缓存中的 Processor ActorRef
    logger.debug("Remove the ModelingProcessorRef which in Worker-Server")
    context.parent ! (conf.id, Opt.CLOSE)
    // 停止Processor Actor
    watcher ! (conf, ProcessModel.MONITOR, Status.STOPPED, message)
    context.stop(self)
    logger.debug(s"modeling processor[${self.path.name}] stopped")
    sys.runtime.gc()
  }

  class ReadExecutor(identifier: String) extends Actor {

    override def receive: Receive = {
      case (datasource: DataSource, parser: Parser[MapRule@unchecked]) =>

        context.actorOf(Props.create(Class.forName(MONITORS.getString(datasource.name)), datasource, parser).
          withDispatcher("akka.dispatcher.monitor"),
          s"monitor-${datasource.name}-${UUID.randomUUID().toString}") ! Opt.START
      case (Some(reader: LogReader[_]), Some(parser: Parser[MapRule@unchecked])) =>
        logger.debug("real preview ")
        val analyzer = ReParserProcessor(ReParser(parser = parser))
        try {

          reader.map {
            case event: Event =>
              /*header master no need*/
              richMap(Map("raw" -> event.content, "identifier" -> identifier))
            case event: Map[String@unchecked, Any@unchecked] =>
              richMap(event) + ("identifier" -> identifier)
          }.map(analyzer.parse).foreach(d => context.actorSelection("/user/publisher") ! d)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            reader.close()
            context.children.foreach(_ ! Opt.STOP)
        } finally {
          reader.close()
          context.children.foreach(_ ! Opt.STOP)
          flinkWatcher ! Opt.STOP
          context.stop(self)
        }

      case Opt.STOP =>
        context.children.foreach(_ ! Opt.STOP)
        flinkWatcher ! Opt.STOP
        context.stop(self)
      case (ProcessModel.MONITOR, Status.UNKNOWN, msg: String) =>
        context.children.foreach(_ ! Opt.STOP)

        flinkWatcher ! Opt.STOP
        context.parent ! (Opt.STOP, s"READ ERROR: $msg")
        context.stop(self)
      case msg =>

        logger.debug(s"ignore message $msg")
    }
  }


  def createReader(datasource: DataSource, parser: Parser[MapRule@unchecked], identifier: String): Unit = {
    context.actorOf(Props.create(classOf[ReadExecutor], this, identifier)) ! (datasource, parser)

  }

  class ModelingExecutor[C <: Analyzer](channel: Channel, analyzer: Analyzer, cluster: Option[FlinkAddress]) extends Actor
    with DAGBuilder[C, BatchTableEnvironment, DataSet[RichMap], DataModelingAnalyzer[C]]
    with Mapper {

    def getRootCause(ex: Throwable): Throwable = {
      if (ExceptionUtils.getRootCause(ex) != null) ExceptionUtils.getRootCause(ex) else ex
    }

    def reader(channel: SingleChannel): FlinkModelingReader = {
      val source = channel.dataSource
      val parser = channel.parser
      val id = channel.id.get
      if (source.name == "hdfs" || source.name.startsWith("es5")) {
        val name: String = Constants.CONF.getString(s"app.modeling.reader.${source.name}")
        val clazz = Class.forName(name).asInstanceOf[Class[FlinkModelingReader]]
        clazz.getConstructor(channel.getClass).newInstance(channel)
      } else {
        //        val props = Properties(Map("identifier" -> id)).properties
        //        val address = RemoteAddressExtension(context.system).address
        //        createReader(source, parser.orNull.asInstanceOf[Parser[MapRule]], id)
        //        new FlinkAkkaModelingReader(SingleChannel(NetSource(protocol = Some(Akka()),
        //          host = address.host,
        //          port = address.port.getOrElse(9093),
        //          properties = props
        //        )))
        throw new NotImplementedError(s"Source type[${source.name}] not support yet. ")
      }


    }


    private val jar: List[String] =JarUtils.jar ++ {
      val path = classOf[ModelingExecutor[_]].getClass.getResource("/").getPath
      val paths = path.split("/")

      (if (path.endsWith("classes/")) {


        val parent = paths.slice(0, paths.length - 3).mkString("/") + "/plugins"


        new File(parent + "elasticsearch_6.x/target").listFiles()
          .filter(file => file.getName.startsWith("sage-bigdata-zeta-es") && file.getName.endsWith(".jar")) ++
          new File(parent + "hdfs/target").listFiles()
            .filter(file => file.getName.startsWith("sage-bigdata-zeta-hdfs") && file.getName.endsWith(".jar"))

      } else {
        new File(paths.slice(0, paths.length - 1).mkString("/") + "/lib").listFiles().filter(
          file => (
            file.getName.startsWith("sage-bigdata-zeta-hdfs") ||
              file.getName.startsWith("sage-bigdata-zeta-es")) && file.getName.endsWith(".jar")
        )
      }).map(_.getAbsolutePath).toList
      //dir.listFiles().foreach(name => logger.info(s"  jar in path  :$name"))
      //dir.listFiles().filter(file => file.getName.startsWith("streaming") && file.getName.endsWith(".jar")).map(_.getAbsolutePath).headOption

    }
    logger.debug(s"jar to submit is:" + jar)

    private final val env = ExecutionEnvironment.createRemoteEnvironment(cluster.get.host, cluster.get.port, jar: _*)

    private val flinkConfig = {
      val conf = new Configuration
      conf.setString(JobManagerOptions.ADDRESS, cluster.get.host)
      conf.setInteger(JobManagerOptions.PORT,cluster.get.port)
      conf
    }
    private val client: ClusterClient[_] = try {
      val _client = cluster.get.application match {
      case Some(x) =>
        new RestClusterClient[String](flinkConfig, x)

      case None =>
        new StandaloneClusterClient(flinkConfig)
    }
    _client.setPrintStatusDuringExecution(false)
    _client
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new ProgramInvocationException("Cannot establish connection to JobManager: " + e.getMessage, e)
    }
    private lazy val output = new AkkaOutputFormat(AkkaUtils.getPath(self)(context.system), conf.channel.`type`.map(_.asInstanceOf[AnalyzerChannel].cluster).orNull, conf.properties)

    private val (job, jobId): (JobGraph, JobID) = {
      try {
        buildWithOut(channel, analyzer)
        // 创建执行计划
        val plan = env.createProgramPlan()
        plan.setJobName(conf.properties.get.getOrElse("wrapper.name", s"Flink Modeling Job") + s"(${TimeUtils.defaultFormat(new Date())})")



        val compiler: Optimizer = new Optimizer(new DataStatistics, new DefaultCostEstimator, flinkConfig)


        // 构建任务
        import org.apache.flink.client.program.ClusterClient.getOptimizedPlan
        val _job = new JobGraphGenerator().compileJobGraph(getOptimizedPlan(compiler, plan, 1), JobID.generate())
        // jar 包收集
        jar.foreach(path => _job.addJar(new Path(new File(path).getAbsoluteFile.toURI)))
        _job.setClasspaths(Collections.emptyList[URL]())
        logger.debug(s"flink modeling jobId = ${_job.getJobID.toString}")
        (_job, _job.getJobID)
      } catch {
        case ex: Throwable =>
          logger.error("构建FlinkJob失败", ex)
          val error = getRootCause(ex)
          context.parent ! (Opt.STOP, s"构建FlinkJob失败: </br> ${error.getMessage}")
          (null, null)
      }
    }

    var runJobActor: ActorRef = _


    override def preStart(): Unit = {
      super.preStart()

      // 提交任务
      try {
        if (jobId != null && job != null) {
          context.parent ! ("modeling", conf.id, jobId.toString)
          runJobActor = context.actorOf(Props.create(classOf[RunFlinkJobActor], this, context.parent), s"${self.path.name}-run")
          runJobActor ! Opt.START
        }
      } catch {
        case ex: Exception =>

          val error = getRootCause(ex)
          logger.error(s"Submit flink job[$jobId] error:", ex)
          context.parent ! (Opt.STOP, error.getMessage)
          context.parent ! ("modeling", conf.id, "")
      }
    }

    def stop(msg: String) {
      if (jobId == null) {
        logger.warn("job id is null, no need to stop, close it directly.")
        self ! (Opt.CLOSE, msg)
      } else {
        try {
          context.parent ! (Opt.CLOSE, jobId.toString, msg)
          /*Await.result(watcherActor ? (jobId, FlinkAPIs.JOB_STATUS), timeout.duration).asInstanceOf[Status] match {
            case Status.RUNNING =>
              logger.info(s"Canceling flink job[$jobId] ")
              client.waitForClusterToBeReady()
              client.endSession(jobId)
              client.cancel(jobId)
              self ! Opt.STOP
            /*case Status.STOPPED =>
              logger.info(s"Flink Job[$jobId] is CANCELLED. ")
              STOPPED ! Opt.STOP
              flinkWatcher = null*/
            case status: Status =>
              logger.warn(s"Flink Job[$jobId] is $status, can't be canceled")
              if (watcherCancellable != null && watcherCancellable.cancel()) {
                watcherCancellable = null
              }
              context.stop(runJobActor)
              client.shutdown()
              output.close()
          }*/
          /*client.waitForClusterToBeReady()
          client.endSession(jobId)
          client.cancel(jobId)
          self ! Opt.STOP*/
        } catch {
          case ex: Exception =>
            logger.error(s"Check the status of flink job[$jobId] error , when stopping ModelingProcessor.", ex)
            self ! (Opt.CLOSE, msg)
        }
      }
    }

    private def close(msg: String): Unit = {
      try {
        if (runJobActor != null)
          context.stop(runJobActor)

        client.shutdown()
        output.close()
      } catch {
        case ex: Exception =>
          logger.error("Executor close error", ex)
      }
      processorExecutor = null
      context.stop(self)
      context.parent ! (Opt.STOP, msg)
    }

    var count = 0L

    override def receive: Receive = {
      case ActorIdentity("output", Some(actor)) =>
        logger.debug(s"output from flink : ${actor} ")
        context.watch(actor)
      case Terminated(actor) =>
        context.unwatch(actor)
        logger.debug("Terminated" + actor)
        sender() ! Opt.CLOSE

      case data: RichMap =>
        count = count + 1
        if (count % 1000 == 0) {
          logger.debug(s"Receive analyzer result : $count ")
        }
        writers.foreach(_ forward data)
      case Opt.STOP =>
        self forward(Opt.STOP, "")
      case (Opt.STOP, msg: String) =>
        logger.debug(s"Stopping modeling executor, jobID[${jobId}], reason: $msg")
        try {
          stop(msg)
          logger.debug(s"Receive analyzer result : $count ")
        } catch {
          case e: Exception =>
            logger.error("Executor stop error", e)
        }
      case Opt.CLOSE =>
        self forward(Opt.CLOSE, "")
      case (Opt.CLOSE, msg: String) =>
        logger.debug(s"Receive analyzer result : $count ")
        logger.debug(s"Closing modeling executor")
        try {
          close(msg)
        } catch {
          case e: Exception =>
            logger.error("Executor close error", e)
        }
    }


    override def engine: String = "modeling"

    override def from(channel: SingleChannel, path: String): DataSet[RichMap] = {
      reader(channel: SingleChannel).execute(env).name(channel.dataSource.uri)
    }

    private def getDataModelingAnalyzer(item: Analyzer, analyzerType: AnalyzerType.Type): DataModelingAnalyzer[C] = {
      val name: String = Constants.CONF.getString(s"app.modeling.analyzer.${item.name}")
      if (name != null) {
        val clazz = Class.forName(name).asInstanceOf[Class[DataModelingAnalyzer[C]]]
        clazz.getConstructor(item.getClass, classOf[AnalyzerType.Type]).newInstance(item, analyzerType)
      } else {
        logger.error(s"unknown analyzer for FlinkStream process :$item ")
        throw new UnsupportedOperationException(s"unknown analyzer for FlinkStream process :${item.getClass} .")
      }
    }

    override def toHandler(item: Analyzer): DataModelingAnalyzer[C] = {
      val analyzerType = if (item == analyzer) {
        conf.channel.`type`.map(_.asInstanceOf[AnalyzerChannel].`type`).getOrElse(AnalyzerType.ANALYZER)
      } else {
        AnalyzerType.ANALYZER
      }
      getDataModelingAnalyzer(item, analyzerType)
    }

    override def toTableHandler(item: SQLAnalyzer): TableDataAnalyzer[BatchTableEnvironment, DataSet[RichMap], org.apache.flink.table.api.Table] = {
      {
        /*val clazz = Class.forName(name).asInstanceOf[Class[DataModelingAnalyzer[_ <: Analyzer]]]
                    clazz.getConstructor(d.getClass).newInstance(d)*/
        /*构建输出处理流*/
        val name: String = Constants.CONF.getString(s"app.modeling.analyzer.${item.name}")
        if (name != null) {
          val clazz = Class.forName(name).asInstanceOf[Class[TableDataAnalyzer[BatchTableEnvironment, DataSet[RichMap], org.apache.flink.table.api.Table]]]
          clazz.getConstructor(item.getClass, classOf[AnalyzerType.Type]).newInstance(item,
            /*
          * 中间过程只能是分析
          * */
            if (item == analyzer) {
              conf.channel.`type`.map(_.asInstanceOf[AnalyzerChannel].`type`).getOrElse(AnalyzerType.ANALYZER)
            } else {
              AnalyzerType.ANALYZER
            })
        } else {
          logger.error(s"unknown analyzer for FlinkStream process :$item ")
          throw new UnsupportedOperationException(s"unknown analyzer for FlinkStream process :${item.getClass} .")
        }

      }
    }

    /**
      * 通道是否用作生产模型
      * 在整个通道的配置中只有最外层的中会有“是否是生产模型”这个配置
      */
    private lazy val isModel = conf.channel.`type`.get.asInstanceOf[AnalyzerChannel].`type` == AnalyzerType.MODEL

    override def sinkOrOutput(t: DataSet[RichMap]): Unit = {
      if (isModel) {
        val data = dataSetWithId(t, analyzer.id)
        data.output(output)
      } else {
        t.output(output)
      }
    }


    /**
      * 对于非SQL的分析 构建处理流
      *
      * @param channel
      * @param analyzer
      * @return
      */
    override def buildNotSQL(channel: Channel, analyzer: Analyzer): List[DataSet[RichMap]] = {
      val handler = toHandler(analyzer)
      if (isModel && handler.modelAble) {
        handler.filter(handler.modelling(buildDag(channel)))
      } else {
        handler.handle(buildDag(channel))
      }
    }

    /**
      * 覆写 toDataSet 方法，在“生产模型”时进行中间模型输出。
      *
      * @param analyzer
      * @param before
      * @return
      */
    override def toDataSet(analyzer: Analyzer)(implicit before: DataSet[RichMap]): DataSet[RichMap] = {
      val handler = toHandler(analyzer)
      // 如果是“生产建模”，重新构建 handler 用来生产模型
      if (isModel && handler.modelAble) {
        val model = handler.modelling(before)
        dataSetWithId(model, analyzer.id).output(output)
        handler.actionWithModel(before, Some(model))
      } else {
        // 中间结果默认分析结果
        handler.action(before)
      }

    }

    /**
      * 在数据集中添加 AnalyzerId 标示
      *
      * @param t  DataSet[RichMap] 数据集
      * @param id Option[String] AnalyzerId
      * @return DataSet[RichMap]  添加了 AnalyzerId 标示的数据集
      */
    def dataSetWithId(t: DataSet[RichMap], id: Option[String]): DataSet[RichMap] = {
      t.map {
        c => c.+("a@id" -> s"${id.get}")
      }(createTypeInformation[RichMap], ClassTag(classOf[RichMap]))
    }

    /**
      * 另启一个Actor 来执行提交，防止 Executor 阻塞
      * RunJobActor 仅仅用于执行提交任务
      **/
    class RunFlinkJobActor(report: ActorRef) extends Actor {
      override def receive: Receive = {
        case Opt.START =>
          try {
            client.setDetached(true)
            client.submitJob(job, env.getJavaEnv.getClass.getClassLoader)
          } catch {
            case ex: Throwable =>
              val error = getRootCause(ex)
              error match {
                case cancel: JobCancellationException =>
                  logger.info(cancel.getMessage)
                case _ =>
                  logger.error(s"Submit flink job[$jobId] error:", ex)
                  if (error.getMessage != null)
                    report ! (Opt.STOP, error.getMessage)
                  else
                    report ! (Opt.STOP, s"$error")
                  report ! ("modeling", conf.id, "")
              }
          }
      }
    }

  }

}
