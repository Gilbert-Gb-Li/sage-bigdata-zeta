package com.haima.sage.bigdata.etl.processor

import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.util.Timeout
import com.codahale.metrics.Meter
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.{executor, _}
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.filter.Rule
import com.haima.sage.bigdata.etl.common.model.{Analyzer, Status, _}
import com.haima.sage.bigdata.etl.metrics.{MeterReport, MetricRegistrySingle}
import com.haima.sage.bigdata.etl.monitor.JoinableMonitor
import com.haima.sage.bigdata.etl.reader.{AkkaLogReader, JDBCLogReader, Position}
import com.haima.sage.bigdata.etl.utils.{ExceptionUtils, Mapper}
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Iterable
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

/**
  * Created by zhhuiyan on 15/1/19.
  */
class Processor(config: Config) extends Actor {

  implicit val timeout = Timeout(5 minutes)
  val watcher: ActorSelection = context.actorSelection("/user/watcher")
  protected val logger: Logger = LoggerFactory.getLogger(classOf[Processor])

  val metric = MetricRegistrySingle.build()

  private var metricNames: List[String] = Nil

  //当所有的读取任务完成后停止数据源标志，默认为false，由TimerTaskServer发起
  //var stopWhenAllFinished = false
  // var processorStopWatcher: Cancellable = _
  //var receivedNewData = false
  var running: Boolean = true
  var inWaiting: Boolean = false
  //写程序错误标示，正常是没有错误
  var fatal = false
  //尝试正常关闭次数
  var finish_count = 0

  //超时时间
  val WAIT_TIME: Long = Try(config.get("timeout_ms", Constants.CONF.getString(PROCESS_WAIT_TIMES)).toLong).getOrElse(PROCESS_WAIT_TIMES_DEFAULT)


  //轮询时间
  val POLL_TIME: Long = Try(config.get("polling_ms", Constants.CONF.getString(PROCESS_WAIT_TIMES)).toLong).getOrElse(PROCESS_WAIT_TIMES_DEFAULT)
  private final val CPU_LOWER: Boolean = Try(config.get("cpu_lower", "false").toBoolean).getOrElse(false)
  private final val ADD_COLLECTOR: Boolean = Try(config.get("collector_info", "false").toBoolean).getOrElse(false)
  private final val ADD_SOURCE: Boolean = Try(config.get("source_info", "false").toBoolean).getOrElse(false)
  private final val ADD_RAW: Boolean = Try(config.get("raw_data", "false").toBoolean).getOrElse(false)
  private final val ADD_PATH: Boolean = Try(config.get("source_path", "false").toBoolean).getOrElse(false)
  private final val ADD_RECEIVE_TIME: Boolean = Try(config.get("receive_time", "false").toBoolean).getOrElse(false)
  private final val ADD_CLUSTER_INFO: Boolean = Try(config.get("cluster_info", "false").toBoolean).getOrElse(false)
  private final val ADD_ANALYZE_TIME: Boolean = Try(config.get("analyze_time", "false").toBoolean).getOrElse(false)


  /* //停止前等待时间（默认30秒）
   val waitBeforeStop: Int = config.timerWrapper match {
     case Some(timerWrapper) =>
       timerWrapper.waitBeforeStop.getOrElse(30)
     case _ => 30
   }*/
  //每次读取条数
  val CACHE_SIZE: Long = CONF.getLong(PROCESS_CACHE_SIZE) match {
    case 0 =>
      PROCESS_CACHE_SIZE_DEFAULT
    case a =>
      a
  }

  //使用的输出个数
  val LEXER_SIZE: Int = CONF.getInt(PROCESS_THREAD_LEXER_SIZE) match {
    case 0 =>
      PROCESS_THREAD_LEXER_SIZE_DEFAULT
    case a =>
      a
  }


  val positive: Boolean = CONF.getString(STORE_POSITION_FLUSH) match {
    case STORE_POSITION_FLUSH_DEFAULT =>
      true
    case _ =>
      false
  }
  //文件读取位置多少条存储一次
  val OFFSET: Int = CONF.getInt(STORE_POSITION_OFFSET) match {
    case 0 =>
      STORE_POSITION_OFFSET_DEFAULT
    case a =>
      a
  }

  //  val datasource: DataSource = config.channel.dataSource match {
  //    case null =>
  //      throw new NullPointerException("datasource must not null")
  //    case ds: KafkaSource => //当数据源是kafka时，如果页面没有设置group_id，那么将数据通道id作为group_id
  //      val group_id = ds.get("group_id", config.id)
  //      ds.copy(properties = ds.properties match {
  //        case Some(p) =>
  //          p.put("group_id", group_id)
  //          Some(p)
  //        case None =>
  //          Properties(Map("group_id" -> group_id)).properties
  //      })
  //    case ds =>
  //      ds
  //  }
  val path = s"${config.id}"

  var monitor: ActorRef = _
  var readers: Map[String, LogReader[_]] = Map()
  final val read_meter: MeterReport = MeterReport(mkMeter(s"${path}_read_success"), None, None)


  /**
    * 构造 Metrics Meter
    *
    * @param name
    * @return
    */
  def mkMeter(name: String): Option[Meter] = {
    if (metricNames.contains(name)) {
      None
    } else {
      metricNames = metricNames.:+(name)
      Some(metric.meter(name))
    }
  }

  //  final val histogram: Histogram = metric.histogram(s"${path}_histogram")

  val writes: List[(String, String, Writer)] = config.writers.zipWithIndex.map {
    case (wc, index) =>
      ( {
        if (wc.id == null || "".equals(wc.id) || wc.isInstanceOf[ForwardWriter])
          s"writer-${wc.name}-$index@${config.id}".replaceAll("[/\\\\]", ".")
        else
          s"${wc.id}@${config.id}".replaceAll("[/\\\\]", ".")
      }, CONF.getConfig("app.writer").getString(wc.name), wc)
  }
  val WRITER_MAX_CACHE = writes.map(_._3.cache).max
  val write_meters: Map[String, MeterReport] = writes.map(name => {
    val n = s"${name._1}_write"
    (n, MeterReport(mkMeter(s"${n}_success"), mkMeter(s"${n}_fail"), None))
  }
  ).toMap
  val writers: List[ActorRef] = writes.map {
    case (name, clazz, wc) =>
      clazz match {
        case msg: String =>
          self ! (name, ProcessModel.WRITER, Status.PENDING)

          logger.debug("writer class:" + msg)
          //优化 ,当数据输出的缓存大于系统配置的缓存时使用系统缓存作为数据输出缓存,加快数据入库速度(不然要等到数据输出触发刷新时间才会读取下一个缓存)
          context.actorOf(Props.create(Class.forName(msg), wc match {
            case w: ES2Writer =>
              w.copy(cache = if (w.cache > CACHE_SIZE) {
                logger.warn(s"writer.cache[${w.cache}] big then process.cache.size[$CACHE_SIZE],use process.cache.size[$CACHE_SIZE].")
                CACHE_SIZE.toInt
              } else {
                w.cache
              })
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


  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(0) {
    case msg =>
      msg.printStackTrace()
      logger.debug(s"error:$msg")
      watcher ! (config.id, ProcessModel.MONITOR, Status.ERROR, msg)
      Stop
  }

  /**
    * 监听器和执行完成 清理持有的对象，设置运行状态，关闭自己
    */
  private def clear(msg: Option[String] = None): Unit = {
    msg match {
      case Some(_) if StringUtils.isNotEmpty(msg.get) => watcher ! (config, ProcessModel.MONITOR, Status.STOPPED, msg.get)
      case _ => watcher ! (config, ProcessModel.MONITOR, Status.STOPPED)
    }
    if (metricNames.nonEmpty) {
      metricNames.foreach {
        name =>
          metric.remove(name)
      }
      metricNames = Nil
    }
    if (positive) {
      readers.foreach {
        data =>
          if (!data._2.isClosed) {
            data._2.close()
          }
      }
    }

    if (context != null && context.parent != null) {
      context.parent ! (config.id, Opt.CLOSE)
      context.stop(self)
      logger.info(s"processor[${self.path.name}] stopped")
      sys.runtime.gc()
    }


  }

  private def stop(msg: Option[String] = None): Unit = {
    running = false

    //关闭监听器
    if (monitor != null) {
      monitor ! STOP
    } else {
      if (context.children.nonEmpty) {
        if (positive) {
          if (readers.isEmpty) {
            context.children.foreach(_ ! Opt.STOP)

          } else {
            readers.foreach {
              data =>
                if (!data._2.isClosed)
                  data._2.close()
            }
          }


        }
      } else {
        clear(msg)
      }
    }
  }

  @volatile
  private var repeat = 1

  val updateParser: Receive = {
    //更新解析规则
    case Some(parser: Parser[_]) =>
      context.children.foreach {
        executor =>
          if (executor != monitor && !writers.contains(executor))
            executor ! parser
      }
  }
  //收到新的任务
  val startOne: Receive = {
    case (Some(reader: LogReader[_]), Some(parser: Parser[_])) =>
      if (running) {
        readers += (reader.path -> reader)
        //设置文件读取状态
        //TODO watcher ! (config, ProcessModel.STREAM, reader.path, Status.PENDING)
        //启动一个执行程序 执行

        context.actorOf(Props.create(classOf[Executor], this, reader, Some(parser), None).withDispatcher("akka.dispatcher.processor"), s"executor-${UUID.randomUUID()}")

        //TODO watcher ! (config, ProcessModel.STREAM, reader.path, Status.RUNNING)
        //文件处理完成

      } else {
        //正在关闭的时候,新来的源直接关闭
        if (!reader.isClosed)
          reader.close()
      }
  }

  val statusProcess: Receive = {
    /*对于flink的流控*/
    case Opt.WAITING =>
      inWaiting = true
    /*对于flink的流控*/
    case (Opt.STOP, Opt.WAITING) =>
      inWaiting = true

    case (path: String, ProcessModel.WRITER, Status.STOPPED) =>
      //所有输出 执行完成
      if (context.children.isEmpty) {
        logger.debug(s" writer[$path] closed")
        watcher ! (config, ProcessModel.WRITER, path, Status.STOPPED)

      }
    //处理writer的状态信息
    case (path: String, ProcessModel.WRITER, status: Status.Status) =>
      watcher ! (config, ProcessModel.WRITER, path, status)
      status match {
        case Status.ERROR =>
          fatal = true
        case _ =>
          fatal = false
      }
    //fatal = false
    //处理writer的状态信息
    case (path: String, ProcessModel.WRITER, status: Status.Status, msg) =>
      //writer 错误标示
      watcher ! (config, ProcessModel.WRITER, path, status, msg)
      status match {
        case Status.RUNNING =>
          fatal = false
        case _ =>
          fatal = true
      }
    // 监听器关闭完成
    case (ProcessModel.MONITOR, Status.CLOSED) =>
      monitor = null
      context.system.scheduler.scheduleOnce(WAIT_TIME milliseconds) {
        logger.debug(s"${config.id} monitor closed, last:{}", context.children.size)
        if (context.children.isEmpty) {
          clear()
        } else {
          if (readers.isEmpty) {
            context.children.foreach(_ ! Opt.STOP)
          } else {
            readers.foreach {
              data =>
                if (!data._2.isClosed)
                  data._2.close()
            }
          }

          self ! Status.CLOSING
        }
      }
    case (ProcessModel.MONITOR, Status.RUNNING) =>
      watcher ! (config, ProcessModel.MONITOR, Status.RUNNING)
    case (ProcessModel.MONITOR, Status.ERROR) =>
      watcher ! (config, ProcessModel.MONITOR, Status.ERROR)
      monitor = null
      if (context.children.isEmpty) {
        clear()
      } else {
        readers.foreach {
          data =>
            if (!data._2.isClosed)
              data._2.close()
        }

        self ! Status.CLOSING
      }
    case (ProcessModel.MONITOR, Status.ERROR, msg: String) =>
      watcher ! (config, ProcessModel.MONITOR, Status.ERROR, msg)
      monitor = null
      if (context.children.isEmpty) {
        clear(Option(msg))
      } else {
        readers.foreach {
          data =>
            if (!data._2.isClosed)
              data._2.close()
        }

        self ! (Status.CLOSING, msg)
      }

    case (ProcessModel.MONITOR, Status.UNKNOWN, msg: String) =>
      logger.info(s"ProcessException:$msg")
      watcher ! (config, ProcessModel.MONITOR, Status.UNKNOWN, msg)
  }

  val operate: Receive = {
    case Status.CLOSING =>
      self ! (Status.CLOSING, "")
    case (Status.CLOSING, msg: String) =>
      if (context.children.size == writers.size) {

        logger.debug("closing writers")
        context.children.foreach(_ ! STOP)
      }

      if ((context.children.nonEmpty && repeat < 1000) && !fatal) {

        if (repeat % 100 == 1)
          logger.debug(s"repeat = $repeat, ${config.id} waiting[$POLL_TIME ms] for children[${context.children.map(_.path.name).mkString(";")}]")
        repeat += 1
        context.system.scheduler.scheduleOnce(POLL_TIME / 100 milliseconds) {
          try {
            if (context != null && context.children != null && context.children.nonEmpty)
              self ! (Status.CLOSING, msg)
            else {
              clear(Option(msg))
            }
          } catch {
            case e: Exception =>
              logger.debug(s" close error:${e.getMessage}")
              clear(Option(msg))
          }


        }
      } else {
        if (fatal)
          logger.warn(s"Because the writer error, stop ${self.path.name} directly.")
        clear(Option(msg))
      }
    case (path: String, Status.FINISHED) =>
      //TODO  watcher ! (config, ProcessModel.STREAM, path, Status.STOPPED)
      //文件处理完成，移除已经完成的执行程序
      readers -= path
      logger.debug(s"reader[$path] process finished")
      //  executors=   executors.filter(_==sender())
      //根据状态判断是清理系统，还是读取下一个文件
      if (!running) {
        if (context.children.isEmpty) {
          clear()
        } else {
          self ! Status.CLOSING
        }
      } else {
        //告诉监听程序，执行完一个文件，可以发送新的文件来处理
        if (monitor != null)
          monitor ! (Opt.DELETE, path)
      }
    //系统启动
    case START =>
      finish_count = 0
      //设置监听状态
      logger.info(s"${self.path.name}:starting")
      try {
        watcher ! (config, ProcessModel.MONITOR, Status.PENDING)


        /*env
        *
        * 每一个tuple-> 返回一个handler
        *
        * */


        config.channel match {
          case SingleChannel(c: Channel, parser, id, collector, channelType) =>
            monitor = context.actorOf(Props.create(classOf[JoinableMonitor]).withDispatcher("akka.dispatcher.monitor"), s"monitor-joinable-${config.id}")
            context.actorOf(Props.create(classOf[ExecutorForStream], this, c,
              parser, channelType.get,
              MeterReport(mkMeter(s"${id.get}@${path}_flinkout_success"),
                None, None)))


          //            monitor = context.actorOf(Props.create(classOf[JoinableMonitor]).withDispatcher("akka.dispatcher.monitor"), s"monitor-joinable-${config.id}")
          //
          //
          //
          //            val _readers = getReader(c).map(tuple => {
          //              ("flink-" + tuple._1, tuple._2)
          //            })
          //
          //
          //            context.actorOf(Props.create(classOf[ExecutorForAnalyzer], this,
          //              c,
          //              parser,
          //              _readers,
          //              channelType.get,
          //              MeterReport(mkMeter(s"${id.get}@${path}_flinkout_success"), None, None)
          //            ).withDispatcher("akka.dispatcher.processor"), s"executor-analyzer-${UUID.randomUUID()}")
          //
          //            logger.debug("readers is:" + _readers.keys)
          //            cacheDags.clear()

          case SingleChannel(dataSource: DataSource, parser, _, _, _) =>
            dataSource.timeout = WAIT_TIME
            val clazz = Class.forName(MONITORS.getString(dataSource.name))
            monitor = context.actorOf(Props.create(clazz, dataSource, parser.orNull).withDispatcher("akka.dispatcher.monitor"), s"monitor-parser-${dataSource.name}-${config.id}")
        }

        //启动创建一个监听器
        if (monitor != null)
          monitor ! config.id
        monitor ! START
      } catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error(s"process start has some error:$e")
          stop(Option(ExceptionUtils.getMessage(e)))
      }
    //系统停止
    case (STOP, msg: String) =>

      self ! (ProcessModel.MONITOR, Status.ERROR, msg: String)
      stop(Option(msg))

    case STOP =>
      logger.debug(s"${self.path.name}: closing")
      stop()
    case msg =>
      logger.warn(s"unknown msg:$msg,sender is [${sender().path}]")
  }

  private lazy val cacheDags: mutable.Map[String, Map[String, ActorRef]] = mutable.HashMap[String, Map[String, ActorRef]]()

  def getReader(channel: Channel, parent: String = ""): Map[String, ActorRef] = {
    channel match {
      case TupleChannel(first, second, _, id) =>
        cacheDags.getOrElseUpdate(id.get, getReader(first, parent + "first-") ++ getReader(second, parent + "second-"))
      case SingleTableChannel(c, table, id) =>
        cacheDags.getOrElseUpdate(id.get, getReader(c, parent + "first-"))
      case SingleChannel(tuple: Channel, _, id, _, _) =>
        cacheDags.getOrElseUpdate(id.get, getReader(tuple, parent))
      case SingleChannel(_: DataSource, _, id, collector, _) =>
        cacheDags.getOrElseUpdate(id.get, Map(parent -> one(id.get, collector.orNull)))
      case TupleTableChannel(f, s, id) =>
        cacheDags.getOrElseUpdate(id.get, getReader(f, parent + "first-") ++ getReader(s, parent + "second-"))
    }
  }

  private def one(id: String, collector: Collector): ActorRef = {
    logger.debug(s"get a akka reader for id[$id]")
    val props = Properties(Map("identifier" -> id)).properties
    val akkaLogReader = new AkkaLogReader(NetSource(Some(Akka()),
      Option(collector.host), collector.port, Array(), None, props), context.system)
    readers += (akkaLogReader.path -> akkaLogReader)
    context.actorOf(Props.create(classOf[Executor],
      this, akkaLogReader, None, Some(MeterReport(mkMeter(s"${id}@${path}_flinkin_success"), None, None))).withDispatcher("akka.dispatcher.processor"), s"executor-analyzer-${UUID.randomUUID()}")

  }

  override def receive: Receive = startOne.orElse(statusProcess).orElse(updateParser).orElse(operate)


  class Executor(reader: LogReader[_], parser: Option[Parser[_ <: Rule]], flinkMeterOpt: Option[MeterReport]) extends Actor with Mapper {

    implicit val positionServer: ActorSelection = context.actorSelection("/user/position")

    //var waitedLexers: mutable.Map[String, ActorRef] = mutable.HashMap()

    var stoppedLexerCount = 0
    protected val logger: Logger = LoggerFactory.getLogger(classOf[Executor])

    //当前输入流使用的解析器
    logger.info(s"start process reader:${reader.path} use parser:${parser.map(_.name).getOrElse("")} by ${self.path}")
    //flowcontrol 流量控制
    /* var flows: Map[String, Agent[Long]] = names.map(name => {
       (name, Agent(0l))
     }
     ).toMap*/

    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(0) {
      case msg =>
        logger.error(s"Executor error:$msg")
        close()
        watcher ! (config.id, ProcessModel.MONITOR, Status.ERROR, msg)
        Stop
    }

    //读取条数技术
    @volatile
    private var count = 0l

    private final val MAX = 1000000000000000000l

    var isUpdate = false

    private val cachePerLexer: Int = (CACHE_SIZE / LEXER_SIZE).toInt


    def children: Iterable[ActorRef] = context.children


    private var processed: Map[String, Long] = if (writers.isEmpty) {
      Map[String, Long]("none" -> 0)
    } else {
      writers.map(w => (w.path.name, 0l)).toMap
    }
    private var batch_number = 0l
    private var un_finish_batch: Map[Long, (ReadPosition, Map[String, Int])] = Map()
    val now_all: Long = System.currentTimeMillis()

    @throws[Exception](classOf[Exception])
    override def preStart(): Unit = {
      super.preStart()


      parser match {
        // analyzer user Executor for Analyzer
        //       case Some(analyzer: Analyzer) =>
        //          logger.debug(s"start flink use [$analyzer]")
        //
        //          /*对于flink 只使用一个解析来保证数据的连续性*/
        //          List({
        //            val flinkWatcher = context.actorOf(Props.create(Class.forName(Constants.CONF.getString(Constants.FLINK_WATCHER_CLASS)), config.channel.`type`.map(_.asInstanceOf[AnalyzerChannel].cluster).get), s"checker-${UUID.randomUUID()}")
        //            val jobManagerConfig = Await.result(flinkWatcher ? FlinkAPIs.JOB_MANAGER_CONFIG, timeout.duration).asInstanceOf[Map[String, String]]
        //            val clusterAddress = s"${jobManagerConfig.getOrElse("jobmanager.rpc.address", "")}:${jobManagerConfig.getOrElse("jobmanager.rpc.port", "")}"
        //            flinkWatcher ! Opt.STOP
        //            val lexer = context.actorOf(Props.create(Class.forName(LEXERS.getString("flink")), channel.get, analyzer, clusterAddress.split(":"), writers).withDispatcher("akka.dispatcher.executor"), s"flink-${config.id}")
        //            lexer ! (Opt.START, POLL_TIME, ADD_CLUSTER_INFO, ADD_ANALYZE_TIME) //启动并设置轮询时间
        //            self ! (lexer.path.name, ProcessModel.LEXER, Status.RUNNING, "")
        //            lexer
        //          })
        case Some(p) =>
          logger.debug(s"start lexer use [$p]")
          val parser_meter: MeterReport = MeterReport(mkMeter(s"${path}_operation_success"), mkMeter(s"${path}_operation_fail"), mkMeter(s"${path}_operation_ignore"), mkMeter(s"${path}_operation_in"))
          (0 until LEXER_SIZE).map {
            i =>
              val lexer = context.actorOf(Props.create(LEXERS(p.getClass), parser.orNull, writers, parser_meter).withDispatcher("akka.dispatcher.executor"), s"lexer-${p.name}-${config.id}-$i")
              lexer ! (Opt.START, POLL_TIME, ADD_COLLECTOR, ADD_RECEIVE_TIME, ADD_SOURCE, ADD_RAW, ADD_PATH, CPU_LOWER) //设置轮询时间
              lexer
          }.toList
        case _ =>
      }

    }


    // val process = context.actorOf(Props.create(classOf[ReaderProcess], this).withDispatcher("akka.dispatcher.executor.process"), "executor.process" + UUID.randomUUID())
    override def receive: PartialFunction[Any, Unit] = {
      case "update" =>
        logger.debug("updating !!" + children.size + "->" + LEXER_SIZE)
        if (children.size == LEXER_SIZE + writers.size) {
          isUpdate = false
          logger.debug("updated ok !!")
        }

      case parser: Parser[_] =>
        logger.debug("update starting !!" + children.size + "->" + LEXER_SIZE)
        isUpdate = true
        context.children.foreach {
          lexer =>
            lexer ! STOP
        }

        val lexerClass = LEXERS(parser.getClass)
        (0 until LEXER_SIZE).map {
          i =>
            context.actorOf(Props.create(lexerClass, parser, writers).withDispatcher("akka.dispatcher.processor"), s"lexer-${parser.name}-${UUID.randomUUID()}-$i")
        }.toList
        context.system.scheduler.scheduleOnce(WAIT_TIME milliseconds) {
          self ! "update"
        }
      /*处理lexer 清除掉的数据，或者没有writer的情况*/
      case ("batch", batch: Long, count: Int) =>

        un_finish_batch.get(batch) match {
          case Some((position, counts)) =>
            un_finish_batch = un_finish_batch + (batch -> (position.copy(), counts.map(tuple => (tuple._1, tuple._2 - count))))
          case _ =>
        }
        batchProcess("none", count)
      case (ref: String, batches: Map[Long@unchecked, Int@unchecked]) =>
        batches.foreach {
          case (batch, number) =>
            un_finish_batch.get(batch) match {
              case Some((position, counts)) =>
                val value = counts.getOrElse(ref, 0) - number
                un_finish_batch = un_finish_batch + (batch -> (position, counts + (ref -> value)))
              case _ =>
            }
        }
        batchProcess(ref, batches.values.sum)
      //读取完成,等待程序完成
      case Status.FINISHED =>
        //出错的时候，不等待
        if ((fatal || un_finish_batch.isEmpty) && context.children.isEmpty) {
          close()
        } else {

          context.system.scheduler.scheduleOnce(10 * WAIT_TIME milliseconds)({
            if (context != null) {
              synchronized[Unit]({
                if (!inFinished) {
                  logger.debug(s"${self.path.toString.split("/").reverse(0)} had wait[${10 * WAIT_TIME} ms],lexers[${context.children.size}], batches[$un_finish_batch] ")
                  inFinished = true
                  if ((fatal || un_finish_batch.isEmpty) && context.children.isEmpty) {
                    close()
                  } else {
                    self ! Status.FINISHED
                  }
                }
              })

            }
          })
          inFinished = false
        }


      //执行读取任务
      case Opt.GET =>

        if (sender().path.toString.contains("flink")) {
          logger.info(s" ${sender().path.name} request")
        } else {
          logger.debug(s" ${sender().path.name} request")
        }
        //                logger.debug(s"Executor: ${sender().path}")
        if (!running) {
          if (!reader.isClosed) {
            logger.info(s" stop reader")

            reader.close()
          }
        }
        val slowProc = processed.values.min

        //配置更新，出错，写数据过慢-> 等待
        if (inWaiting || isUpdate || fatal || count - slowProc >= CACHE_SIZE) {
          if (WRITER_MAX_CACHE >= CACHE_SIZE) {
            sender ! (WAITING, FLUSH)
          } else {
            sender ! WAITING
          }

        } else {
          if (slowProc >= MAX) {
            count = count - MAX
            processed = processed.map(tuple => (tuple._1, tuple._2 - MAX))
            logger.info(s"${reader.path} processed :100T(百万亿条)")
          }

          val (next: Iterable[Any], timeOut: Boolean) = reader.take(WAIT_TIME, cachePerLexer)

          if (next.isEmpty && !timeOut) {


            /*数据读取完成*/
            /*切换文件*/
            logger.debug(s"to stop lexer[${sender()}]")
            sender ! STOP
            if (!inFinished) {
              inFinished = true
              self ! Status.FINISHED
            }
          } else if (next.nonEmpty) {
            batch_number += 1
            if (sender().path.toString.contains("flink")) {
              logger.info(s" ${sender().path.name} take data:${next.size}")
            } else {
              logger.debug(s" ${sender().path.name} take data:${next.size}")
            }


            increment(reader, next.size)
            /*if (CPU_LOWER){
              logger.debug(s"lower cpu is running")
              //TimeUnit.MILLISECONDS.sleep(WAIT_TIME)
            }*/
            sender ! (reader.path, batch_number, next)
          }
          if (timeOut) {
            /*没有新数据到来刷新缓存*/
            // receivedNewData = false
            reader match {
              case r: JDBCLogReader if r.hasError._1 =>
                watcher ! (config, ProcessModel.STREAM, reader.path, Status.ERROR, r.hasError._2)
              case _ =>
            }
            sender ! (WAITING, FLUSH)
          }


        }

      case (Opt.STOP, msg: String) =>
        watcher ! (config, ProcessModel.LEXER, path, Status.ERROR, msg)
      // context.parent ! (Opt.STOP, msg: String)
      case (Opt.CLOSE, lexer: ActorRef) =>
        logger.debug(s" stop lexer" + lexer)
        lexer ! STOP
      case ("flink", jobId: String) =>
        context.actorSelection("/user/server") ! ("analyzer", config.id, jobId)
      case (path: String, ProcessModel.LEXER, status: Status.Status, msg) =>
        //analysis 错误标示
        watcher ! (config, ProcessModel.LEXER, path, status, msg)
      /*status match {
        case Status.RUNNING =>
          fatal = false
        case _ =>
          fatal = true
      }*/
      //Flink Metrics
      case (MetricPhase.FLINKIN, count: Int) =>
        flinkMeterOpt match {
          case Some(flinkMeter) =>
            if (flinkMeter.success.nonEmpty) flinkMeter.success.get.mark(count)
          case None =>
            logger.warn(s"FLink meter is none, MetricPhase = ${MetricPhase.FLINKIN}, count = $count.")
        }
      case msg =>
        logger.warn(s"unknown msg:$msg,sender is [${sender().path}]")
    }


    @volatile
    private var inFinished = false

    def close(): Unit = {

      synchronized[Unit]({
        if (!reader.isClosed) {
          reader.close()
        }
        if (context != null && context.parent != null) {
          logger.info(s"${reader.path} total read :$count;take ${System.currentTimeMillis() - now_all} millis;finished=$running")
          context.parent ! (reader.path, Status.FINISHED)
          context.stop(self)
          logger.debug(s"${self.path.name} stopped")
          // sys.runtime.gc()
        }
      })

    }

    //检查批次是否处理完成，并持久化
    def batchProcess(ref: String, number: Int): Unit = {
      //处理缓存计数
      if (processed.contains(ref)) {
        val value = processed.getOrElse(ref, 0l) + number
        processed = processed + (ref -> value)
      } else {
        //for  remove not belong to any writer
        processed = processed.map(tuple => (tuple._1, tuple._2 + number))
      }
      //处理数据处理的进度
      if (!positive) {
        var real: Long = 0
        var pos: ReadPosition = null

        if (un_finish_batch.nonEmpty) {
          var min: Long = un_finish_batch.keys.min
          while ( {
            if (un_finish_batch.isEmpty) {
              false
            } else {
              un_finish_batch.get(min) match {
                case Some((position, counts)) if !counts.exists(_._2 > 0) =>
                  pos = position
                  true
                case _ =>
                  false
              }
            }
          }) {
            real = min
            un_finish_batch = un_finish_batch - min
            min = min + 1
          }
        }


        if (pos != null) {
          reader match {
            case callback: Position =>
              //logger.debug(s" persistent batch[$real] callback[${pos.position}]")
              if (inFinished && un_finish_batch.isEmpty) {
                callback.callback(pos)
                if (context.children.isEmpty) {
                  close()
                } else {
                  self ! Status.FINISHED
                }
              } else {
                callback.callback(pos)
              }

            case _ =>
              logger.warn(s"class[${reader.getClass}] not support  position server")
          }
        }

      }
    }

    //输入流位置信息记录处理
    def increment(obj: Any, n: Int): Unit = {
      count += n
      reader match {
        case position: Position =>
          if (positive) {
            reader match {
              case rabbitmq: Position =>
                rabbitmq.callback(position.position.copy())
              case _ =>
                logger.warn(s"class[${reader.getClass}] not support  position server")
            }
          } else {
            if (writers.nonEmpty) {
              val counts = writers.map {
                ref => (ref.path.name, n)
              }.toMap
              if (read_meter.success.nonEmpty) read_meter.success.get.mark(n)
              un_finish_batch = un_finish_batch + (batch_number -> (position.position.copy(), counts))
            } else {
              un_finish_batch = un_finish_batch + (batch_number -> (position.position.copy(), Map[String, Int]("all" -> n)))
            }
          }
        case _ =>
      }
    }
  }

  class ExecutorForStream(channel: Channel,
                          _analyzer: Option[Analyzer],
                          channelType: AnalyzerChannel, flinkMeter: MeterReport) extends Actor with Mapper {
    protected val logger: Logger = LoggerFactory.getLogger(classOf[ExecutorForAnalyzer])
    //当前输入流使用的解析器
    //logger.info(s"start process akka:${first.path} join akka:${second.path} use analyzer:${analyzer.name} by ${self.path}")


    @throws[Exception](classOf[Exception])
    override def preStart(): Unit = {
      super.preStart()
      _analyzer match {
        case Some(analyzer) =>
          val lexer = context.actorOf(Props.create(Class.forName("com.haima.sage.bigdata.analyzer.streaming.processor.StreamingProcessor"), channel, analyzer, channelType.cluster, writers).withDispatcher("akka.dispatcher.executor"), s"stream-${config.id}")
          lexer ! (Opt.START, POLL_TIME, ADD_CLUSTER_INFO, ADD_ANALYZE_TIME) //启动并设置轮询时间
          self ! (lexer.path.name, ProcessModel.LEXER, Status.RUNNING, "")
        case None =>
          logger.warn(s"not fount an analyzer for flink to executor")

      }
    }


    // val process = context.actorOf(Props.create(classOf[ReaderProcess], this).withDispatcher("akka.dispatcher.executor.process"), "executor.process" + UUID.randomUUID())
    override def receive: PartialFunction[Any, Unit] = {
      case Status.STOPPED =>
        context.stop(self)
      case Opt.STOP =>

        logger.info(s"to stop lexer ${context.children}")
        context.children.foreach(_ ! Opt.STOP)
      case (Opt.STOP, msg) =>
        context.parent ! (Opt.STOP, msg)
      case (Opt.CLOSE, lexer: ActorRef) =>
        logger.info(s" stop lexer" + lexer)
        lexer ! STOP
      case ("flink", jobId: String) =>
        context.actorSelection("/user/server") ! ("analyzer", config.id, jobId)
      case (path: String, ProcessModel.LEXER, status: Status.Status, msg) =>
        watcher ! (config, ProcessModel.LEXER, path, status, msg)
        status match {
          //          case Status.RUNNING =>
          //            fatal = false
          case Status.ERROR =>
            fatal = true
            context.parent ! (Opt.STOP, msg)
          case _ =>

        }
      case (path: String, Status.FINISHED) =>
        context.parent ! (path, Status.FINISHED)
      //Flink Metrics
      case (MetricPhase.FLINKIN, count: Int) =>
        /*
        * FIXME flink in meter
        * */

        if (flinkMeter.in.nonEmpty) flinkMeter.in.get.mark(count)
      case (MetricPhase.FLINKOUT, count: Int) =>
        if (flinkMeter.success.nonEmpty) flinkMeter.success.get.mark(count)
      //执行读取任务
      case msg =>
        logger.warn(s"ignore msg:$msg")
    }
  }


  class ExecutorForAnalyzer(channel: Channel,
                            _analyzer: Option[Analyzer],
                            from: Map[String, ActorRef],
                            channelType: AnalyzerChannel, flinkMeter: MeterReport) extends Actor with Mapper {


    protected val logger: Logger = LoggerFactory.getLogger(classOf[ExecutorForAnalyzer])


    logger.debug(s"from:[${from.keys}]")
    //当前输入流使用的解析器
    //logger.info(s"start process akka:${first.path} join akka:${second.path} use analyzer:${analyzer.name} by ${self.path}")


    def children: Iterable[ActorRef] = context.children

    val now_all: Long = System.currentTimeMillis()


    @throws[Exception](classOf[Exception])
    override def preStart(): Unit = {
      super.preStart()
      _analyzer match {
        case Some(analyzer) =>
          val lexer = context.actorOf(Props.create(Class.forName("com.haima.sage.bigdata.analyzer.streaming.lexer.FlinkLexer"), channel, analyzer, channelType.cluster, writers).withDispatcher("akka.dispatcher.executor"), s"flink-${config.id}")
          lexer ! (Opt.START, POLL_TIME, ADD_CLUSTER_INFO, ADD_ANALYZE_TIME) //启动并设置轮询时间
          self ! (lexer.path.name, ProcessModel.LEXER, Status.RUNNING, "")
        case None =>
          logger.warn(s"not fount an analyzer for flink to executor")

      }
    }


    // val process = context.actorOf(Props.create(classOf[ReaderProcess], this).withDispatcher("akka.dispatcher.executor.process"), "executor.process" + UUID.randomUUID())
    override def receive: PartialFunction[Any, Unit] = {
      case Status.STOPPED =>
        context.stop(self)
      case (Opt.STOP, msg) =>
        context.parent ! (Opt.STOP, msg)
      case (Opt.CLOSE, lexer: ActorRef) =>
        logger.info(s" stop lexer" + lexer)
        lexer ! STOP
      case ("flink", jobId: String) =>
        context.actorSelection("/user/server") ! ("analyzer", config.id, jobId)
      case (path: String, ProcessModel.LEXER, status: Status.Status, msg) =>
        watcher ! (config, ProcessModel.LEXER, path, status, msg)
        status match {
          //          case Status.RUNNING =>
          //            fatal = false
          case Status.ERROR =>
            fatal = true
            context.parent ! (Opt.STOP, msg)
          case _ =>

        }
      case (path: String, Status.FINISHED) =>
        context.parent ! (path, Status.FINISHED)
      //Flink Metrics

      case (MetricPhase.FLINKOUT, count: Int) =>
        if (flinkMeter.success.nonEmpty) flinkMeter.success.get.mark(count)
      //执行读取任务
      case msg =>
        val path = sender().path.name
        from.filter {
          case (key, _) =>
            path.startsWith(key)
        }.toList match {
          case head :: Nil =>
            head._2 forward msg
          case _ =>
            logger.warn(s"unknown msg:$msg, sender is [${sender().path}]")
        }
    }


  }

}




