package com.haima.sage.bigdata.analyzer.streaming.lexer

import java.io.File
import java.net.URL
import java.util.{Collections, Date, UUID}

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.analyzer.dag.DAGBuilder
import com.haima.sage.bigdata.analyzer.streaming.DataStreamAnalyzer
import com.haima.sage.bigdata.analyzer.streaming.side.SideExec
import com.haima.sage.bigdata.analyzer.streaming.source.{AkkaSink, AkkaSource}
import com.haima.sage.bigdata.analyzer.utils.JarUtils
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.base.Lexer
import com.haima.sage.bigdata.etl.common.model.Status.Status
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.plugin.flink.{FlinkAddress, FlinkStatusProcessor}
import com.haima.sage.bigdata.etl.utils.{AkkaUtils, ExceptionUtils, HTTPAddress, KnowledgeUsersFactory}
import org.apache.flink.client.program.rest.RestClusterClient
import org.apache.flink.client.program.{ClusterClient, ProgramInvocationException}
import org.apache.flink.configuration.{AkkaOptions, Configuration, JobManagerOptions, RestOptions}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment

import scala.concurrent.Await
import scala.util.Try

/**
  * 构建flink 任务,并提交
  *
  * @param analyzer
  * @param writers
  */
class FlinkLexer(channel: Channel, val analyzer: Analyzer, _cluster: String, val writers: List[ActorRef])
  extends Lexer[DataStream[RichMap], DataStream[RichMap]]
    with DAGBuilder[Analyzer, StreamTableEnvironment, DataStream[RichMap], DataStreamAnalyzer[Analyzer, _, _, _ <: Any]]
    with Actor with FlinkStatusProcessor {

  import scala.concurrent.duration._


  implicit val timeout: Timeout = Timeout(5 minutes)
  private implicit lazy val system: ActorSystem = context.system


  val flinkWatcher: ActorRef = context.actorOf(Props.create(Class.forName(Constants.CONF.getString(Constants.FLINK_WATCHER_CLASS)),
    _cluster), s"checker-${UUID.randomUUID()}")

  val cluster: Option[FlinkAddress] = Await.result(flinkWatcher ? FlinkAPIs.RPC_ADDRESS, timeout.duration).asInstanceOf[Option[FlinkAddress]]


  /* 使用的jar包 */
  private val jar: List[String] = JarUtils.jar ++ {
    val path = classOf[FlinkLexer].getClass.getResource("/").getPath
    val paths = path.split("/")

    (if (path.endsWith("classes/")) {


      val parent = paths.slice(0, paths.length - 3).mkString("/") + "/plugins/"


      new File(parent + "elasticsearch_6.x/target").listFiles()
        .filter(file => file.getName.startsWith("sage-bigdata-zeta-es") && file.getName.endsWith(".jar"))

    } else {
      new File(paths.slice(0, paths.length - 1).mkString("/") + "/lib").listFiles().filter(
        file => file.getName.startsWith("sage-bigdata-zeta-es") && file.getName.endsWith(".jar")
      )
    }).map(_.getAbsolutePath).toList

  }
  private final val env = StreamExecutionEnvironment.createRemoteEnvironment(cluster.get.host, cluster.get.port, jar: _*)

  /* flink输出任务*/
  def sink: AkkaSink = {
    new AkkaSink(AkkaUtils.getPath(self))
  }


  private val client: ClusterClient[_] = try {
    val conf = new Configuration
    conf.setString(JobManagerOptions.ADDRESS, cluster.get.host)
    conf.setInteger(JobManagerOptions.PORT, cluster.get.port)
    conf.setString(RestOptions.ADDRESS, HTTPAddress(_cluster).get.host)
    conf.setInteger(RestOptions.PORT, HTTPAddress(_cluster).get.port)
    conf.setString(AkkaOptions.LOOKUP_TIMEOUT, "6000 s")



    //val executor= new RemoteExecutor(cluster.get.host, cluster.get.port,jar)

    logger.debug("cluster is " + cluster)
    val _client = cluster.get.application match {
      case Some(x) =>
        new RestClusterClient[String](conf, x)

      case None =>
        // new RestClusterClient[String](conf, "RemoteExecutor")
        //  new StandaloneClusterClient(conf)
        new RestClusterClient[String](conf, "RemoteStreamEnvironment");
    }
    _client.setPrintStatusDuringExecution(false)
    _client


  } catch {
    case e: Exception =>
      logger.error(ExceptionUtils.getMessage(e))
      throw new ProgramInvocationException("Cannot establish connection to JobManager: " + e.getMessage, e)
  }

  private val (job, jobId) = {
    try {
      buildWithOut(channel, analyzer)


      val _job = env.getJavaEnv.getStreamGraph.getJobGraph
      _job.setSavepointRestoreSettings(SavepointRestoreSettings.none)
      jar.foreach(path => _job.addJar(new Path(new File(path).getAbsoluteFile.toURI)))
      _job.setClasspaths(Collections.emptyList[URL]())
      val id = _job.getJobID
      logger.debug(s"stream job[${id.toString}]")
      (_job, id)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        logger.error("Flink job error", e)
        context.parent ! (self.path.name.toString, ProcessModel.LEXER, Status.ERROR, s"FLINK COMMIT ERROR:${ExceptionUtils.getMessage(e)}")
        (null, null)
    }
  }

  val thread: Thread = new Thread() with Serializable {
    override def run(): Unit = {
      try {
        if (job != null && env != null) {
          client.setDetached(true)
          client.submitJob(job, env.getJavaEnv.getClass.getClassLoader)
        }
      } catch {
        case e: ProgramInvocationException =>
          e.getCause.getCause match {
            case ne: NoResourceAvailableException =>
              context.parent ! (self.path.name, ProcessModel.LEXER, Status.ERROR, s"FLINK COMMIT ERROR:${ExceptionUtils.getMessage(ne)}")
            case e: Throwable =>
              e.printStackTrace()
              context.parent ! (self.path.name, ProcessModel.LEXER, Status.ERROR, s"FLINK COMMIT ERROR:${ExceptionUtils.getMessage(e)}")
              logger.error("FLINK COMMIT ERROR", e)
            case ex =>
              context.parent ! (self.path.name, ProcessModel.LEXER, Status.ERROR, s"FLINK COMMIT ERROR:${ExceptionUtils.getMessage(ex)}")
              logger.error("FLINK COMMIT ERROR", ex)
          }
        case e: Exception =>
          logger.error("FLINK COMMIT ERROR", e)
      }
    }
  }


  private def writerWithDefaultBatch(log: RichMap): Unit = {

    logger.info(s" flink lexer send data ")
    writers.foreach(_ forward(1L, List(log)))
  }


  def active(jobId: String, wait: Long, addCluster: Boolean, addTime: Boolean): Receive = apply(jobId.toString).orElse {

    case "flink-sink" =>
      logger.debug(s"sink from flink : ${sender()} ")
      context.watch(sender())
    case Terminated(actor) =>
      context.unwatch(actor)
      logger.warn("Terminated:" + actor)


    case (Opt.START, wait: Long, addCluster: Boolean, addTime: Boolean) =>
      if (jobId != null) {
        thread.start()
        context.unbecome()
        context.become(active(jobId.toString, wait, addCluster, addTime))
        sender() ! ("flink", jobId.toString)
      }

    case ("source", Status.RUNNING) =>
      sender() ! (Opt.PUT, wait)

    case (Opt.STOP, msg: String) =>
      context.parent ! (Opt.STOP, msg: String)
    case Opt.STOP =>
      flinkWatcher ! (jobId.toString, FlinkAPIs.JOB_CONCELLATION)
      import  context.dispatcher
      context.system.scheduler.scheduleOnce(1000 milliseconds) {
        try {
          logger.info(s"Cancelling flink job[$jobId]. ")
          Await.result(flinkWatcher ? (jobId.toString, FlinkAPIs.JOB_STATUS), timeout.duration).asInstanceOf[Status] match {
            case Status.UNAVAILABLE =>
              logger.warn(s"connect to flink fail when closing job[$jobId], try again later.")
              self ! Opt.STOP
            case Status.RUNNING =>
              logger.info(s"Receive status[RUNNING] when closing job[$jobId], try again later.")
              self ! Opt.STOP
            case status =>
              logger.info(s"Receive status[$status], when closing job[$jobId]. Start to close FlinkLexer.")
              self ! Opt.CLOSE
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Closing job[$jobId] error", ex)
            self ! Opt.CLOSE
            close()
        }
      }
    case Opt.CLOSE =>
      close()
    case ("sink", Opt.CLOSE) =>
      writers.foreach(_ forward Opt.FLUSH)
      logger.info(s"flink lexer[${self.path.name}] closed")
      context.stop(self)
    case value: Map[String@unchecked, Any@unchecked] =>
      logger.info(s"flink analyze data $value")


      val parsedWithCluster = if (addCluster) {
        value + ("a@cluster" -> cluster)
      } else {
        value
      }
      val parsedWithAnalyzeTime = if (addTime) {
        parsedWithCluster + ("a@analyze_time" -> new Date())
      } else {
        parsedWithCluster
      }
      writerWithDefaultBatch(parsedWithAnalyzeTime)

    case Opt.FLUSH =>
      writers.foreach(_ forward Opt.FLUSH)

    case obj@(_: MetricPhase.MetricPhase, _: Int) =>
      context.parent.forward(obj)
    case msg =>
      logger.warn(s"unknown msg:$msg,sender is [${sender().path}]")

  }


  override def receive: Receive = active(jobId.toString,
    Try(Constants.CONF.getLong(Constants.PROCESS_WAIT_TIMES)).getOrElse(Constants.PROCESS_WAIT_TIMES_DEFAULT),
    Try(Constants.CONF.getBoolean(Constants.ADD_CLUSTER_INFO)).getOrElse(Constants.ADD_CLUSTER_INFO_DEFAULT),
    Try(Constants.CONF.getBoolean(Constants.ADD_ANALYZE_TIME)).getOrElse(Constants.ADD_ANALYZE_TIME_DEFAULT))

  def close() {

    try {
      writers.foreach(_ forward Opt.FLUSH)
      if (jobId == null) {
        logger.info("job id is null can't to stop it ")
      } else {

        /*logger.info(s"cancel job by id[$jobId] ")
        client.waitForClusterToBeReady()
        client.endSession(jobId)
        client.cancel(jobId)*/
        client.shutdown()
        if (thread.isAlive)
          thread.interrupt()
      }

      sink.close()
    } catch {
      case e: Exception =>
        logger.debug(e.getMessage)
    } finally {
      logger.info("flink lexer closed")
      flinkWatcher ! Opt.STOP
      context.parent ! Status.STOPPED
      context.stop(self)

    }
  }

  override def parse(from: DataStream[RichMap]): DataStream[RichMap] = {
    null
  }

  override def engine: String = "streaming"

  override def from(channel: SingleChannel, path: String): DataStream[RichMap] = {
    val source = new AkkaSource(path = AkkaUtils.getPath(context.parent), lexer = AkkaUtils.getPath(self), path + "end")
    env.addSource[RichMap](source)
  }

  override def toHandler(_analyzer: Analyzer): DataStreamAnalyzer[Analyzer, _, _, _ <: Any] = {
    val name = Constants.CONF.getString(s"app.streaming.analyzer.${_analyzer.name}")
    if (name != null) {
      val knowledgeUser = _analyzer.useModel.map(id => AkkaUtils.getPathString(KnowledgeUsersFactory.get(id)(context.system))(context.system))
      val clazz = Class.forName(name).asInstanceOf[Class[DataStreamAnalyzer[Analyzer, _, _, _ <: Any]]]
      clazz.getConstructor(_analyzer.getClass).newInstance(_analyzer.withModel(knowledgeUser.orNull))
    } else {
      logger.error(s"not found analyzer for Flink Stream process :${_analyzer} ")
      throw new UnsupportedOperationException(s"not found analyzer for Flink Stream process :${_analyzer.getClass} .")
    }
  }

  override def toTableHandler(sqlAnalyzer: SQLAnalyzer): TableDataAnalyzer[StreamTableEnvironment, DataStream[RichMap], org.apache.flink.table.api.Table] = {
    val name: String = Constants.CONF.getString(s"app.streaming.analyzer.${sqlAnalyzer.name}")
    if (name != null) {
      val clazz = Class.forName(name).asInstanceOf[Class[TableDataAnalyzer[StreamTableEnvironment, DataStream[RichMap], org.apache.flink.table.api.Table]]]
      clazz.getConstructor(sqlAnalyzer.getClass).newInstance(sqlAnalyzer)
    } else {
      logger.error(s"unknown sql analyzer for FlinkStream process :$sqlAnalyzer ")
      throw new UnsupportedOperationException(s"unknown sql analyzer for FlinkStream process :${sqlAnalyzer.getClass} .")
    }
  }


  override def sinkOrOutput(t: DataStream[RichMap]): Unit = {

    logger.debug(s" add sink is started ")
    t.addSink(sink)


  }

  override def build(channel: Channel, analyzer: Analyzer): List[DataStream[RichMap]] = {
    analyzer match {
      case sqlAnalyzer: SQLAnalyzer =>
        val handler = toTableHandler(sqlAnalyzer)
        val sources = buildTableDag(channel.asInstanceOf[TableChannel])
        implicit val environment: StreamTableEnvironment = handler.getEnvironment(sources.head._1)
        val registeredTable = sources.filter(!_._2.isSideTable).map(d => (d._2.tableName.toUpperCase, (handler.register(d._1, d._2)(environment), Option(d._1)))).toMap
        val sideExec = {
          val name: String = Constants.CONF.getString(s"app.flink.sql.sink.exec")
          val clazz = Class.forName(name).asInstanceOf[Class[SideExec[StreamTableEnvironment]]]
          clazz.getConstructor().newInstance()
        }
        val tables = sideExec.parseTables(channel.asInstanceOf[TableChannel])
        val fSql = sideExec.exec(sqlAnalyzer.sql, tables, registeredTable, environment)

        val fieldsDic = tables.flatMap(_._1.fields.map(d => (d._1.toUpperCase, d._1)))
        handler.execute(environment, fSql, Some(fieldsDic))
      case _ =>
        /*构建输出处理流*/
        buildNotSQL(channel, analyzer)
    }
  }
}
