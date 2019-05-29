package com.haima.sage.bigdata.etl.lexer

import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{Actor, ActorRef}
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.base.Lexer
import com.haima.sage.bigdata.etl.common.model.filter.{Filter, MapRule}
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import com.haima.sage.bigdata.etl.normalization.Normalizer
import com.haima.sage.bigdata.etl.utils.Mapper

import scala.concurrent.duration._
import scala.util.Try


/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/28 13:04.
  */


abstract class DefaultLexer[F] extends Lexer[F, RichMap] with Actor with Normalizer with Mapper {
  def parser: Parser[MapRule]

  def writers: List[ActorRef]

  def report: MeterReport

  override def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  protected lazy val filter: Option[Filter] = Option(parser.filter) match {
    case Some(f) if f.nonEmpty =>
      Some(Filter(f.asInstanceOf[Array[MapRule]]))
    case _ =>
      None
  }
  //使用的输出个数
  val LEXER_SIZE: Int = CONF.getInt(PROCESS_THREAD_LEXER_SIZE) match {
    case 0 =>
      PROCESS_THREAD_LEXER_SIZE_DEFAULT
    case a =>
      a
  }

  /*
  *
  * 删除特殊字段`_`开头的
  * */
  final def removeNUll(map: RichMap): RichMap = {
    map.filter {
      case (_, value) => value != null
    }
  }


  //等待时间
  private val WAIT_TIME: Long = Try(CONF.getLong(PROCESS_WAIT_TIMES)).getOrElse(PROCESS_WAIT_TIMES_DEFAULT)

  final val RAW: String = "raw"
  // val maker = TimestampMaker()
  private val CPU_LOWER = false
  private val ADD_COLLECTOR = Try(CONF.getBoolean(ADD_COLLECTOR_INFO)).getOrElse(false)
  private val ADD_SOURCE = Try(CONF.getBoolean(ADD_SOURCE_INFO)).getOrElse(false)
  private val ADD_RAW = Try(CONF.getBoolean(ADD_RAW_DATA)).getOrElse(false)
  private val ADD_PATH: Boolean = false
  private val ADD_TIME = Try(CONF.getBoolean(ADD_RECEIVE_TIME)).getOrElse(false)
  private val done: AtomicBoolean = new AtomicBoolean(false)
  private lazy val hostname: String = CONF.getString("akka.remote.netty.tcp.hostname")
  private lazy val port: String = CONF.getString("akka.remote.netty.tcp.port")

  def name: String = parser.name


  //  val now = System.currentTimeMillis()

  def write(batch: Long, log: List[RichMap]): Unit = {


    writers.foreach(_ forward ((batch: Long, log: List[RichMap])))
  }

  // private var task: Cancellable = _


  def schedule(): Unit = {
    /* if (task != null && !task.isCancelled) {
       task.cancel()
     }
     task = */ context.system.scheduler.scheduleOnce(WAIT_TIME milliseconds) {
      if (!done.get() && context != null && context.parent != null) {
        context.parent ! Opt.GET
      }

    }
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    //   logger.debug(s"lexers start ---"+self.path)
  }

  override def receive: Receive = onMessage(WAIT_TIME, ADD_COLLECTOR, ADD_TIME, ADD_SOURCE, ADD_RAW, ADD_PATH, CPU_LOWER)

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    //  TimestampMaker.close(maker)
  }

  private def onMessage(wait: Long,
                        add_collector: Boolean,
                        add_receive_time: Boolean,
                        add_source: Boolean,
                        add_raw: Boolean,
                        add_path: Boolean,
                        cpu_lower: Boolean): Receive = {
    case Opt.STOP =>
      done.set(true)
      writers.foreach(_ forward Opt.FLUSH)
      logger.debug(s"${self.path.name} closed")
      context.stop(self)
    case (Opt.START, wait: Long, add_collector: Boolean, add_receive_time: Boolean, add_source: Boolean, add_raw: Boolean, add_path: Boolean, cpu_lower: Boolean) =>
      context.become(onMessage(wait: Long, add_collector: Boolean, add_receive_time: Boolean, add_source: Boolean, add_raw: Boolean, add_path: Boolean, cpu_lower: Boolean))
      sender() ! Opt.GET
    case (Opt.WAITING, Opt.FLUSH) =>
      writers.foreach(_ forward Opt.FLUSH)
      schedule()
    case Opt.WAITING =>
      schedule()

    case (path: String, batch: Long, events: List[F]) =>
      val now = System.currentTimeMillis()
      var removed = 0
      var isEvent = true

      var count = 0
      report.in.foreach(_.mark(events.size))
      events.foreach { event =>
        try {
          val parsed: RichMap = event match {
            case evt: Event =>
              val _parsed = parse(event)


              /*
              * 添加数据源相关的信息
              * */
              val parsedWithAttr: RichMap = _parsed ++ evt.header.map(_.map {
                case (key, v) =>
                  ("c@source_" + key, v)
              }).getOrElse(RichMap())
              /*
               *添加原始信息
               *
               */

              parsedWithAttr.+("c@raw" -> evt.content)


            case evt: RichMap =>

              isEvent = false
              evt
          }

          val withInfo = parsed +
            ("c@path" -> path) +
            ("c@collector" -> s"akka.tcp://$hostname:$port") +
            ("c@receive_time" -> new Date())

          val filtered = filter.map(_.filter(withInfo)).getOrElse(List(withInfo)).map(t => {
            val normalized: Map[String, Any] = normalize(t - "c@raw")

            // 计量解析成功、失败信息


            val data: Map[String, Any] = removeNUll(normalized)

            def isError = data.contains("error")

            data.filter {
              case ("c@collector", _) if !add_collector =>
                false
              case ("c@path", _) if !add_path =>
                false
              case ("c@receive_time", _) if !add_receive_time =>
                false
              case ("c@raw", _) if !(add_raw && isEvent && parser.name != "nothing" && parser.name != "transfer" && !isError) =>

                false
              case (key, _) if !add_source && key.startsWith("c@source") =>
                false

              case _ =>
                true

            }
          }).filter(_.nonEmpty)

          if (filtered.isEmpty) {
            removed += 1
            // 计量解析忽略信息
            if (report.ignore.nonEmpty) report.ignore.get.mark(1)
          } else {
            val error = filtered.count(_.contains("error"))
            if (error > 0) {
              report.fail.foreach(_.mark(error))
            }
            report.success.foreach(_.mark(filtered.size - error))

            write(batch, filtered.map(dat => {
              {
                /*
                * FIXME 包含raw字段时会覆盖掉
                * */

                val f: RichMap = if (dat.contains("c@raw")) {
                  dat.filter(_._1 != RAW).map {
                    case ("c@raw", value) =>
                      (RAW, value)
                    case (key, value) =>
                      (key, value)
                  }
                } else {
                  dat
                }
                f
              }
            })
            )

          }
          if (logger.isDebugEnabled)
            count += filtered.size
        } catch {
          case e: Exception =>
            logger.error(s"lexer parser has error:$e")
            val ert: RichMap = event match {
              case Event(header, content) =>
                header match {
                  case Some(d: Map[String, Any]) =>
                    Map(RAW -> content, "c@error" -> e.getMessage) ++ d
                  case _ =>
                    Map(RAW -> content, "c@error" -> e.getMessage)
                }

              case data: RichMap =>
                data + ("c@error" -> e.getMessage)
            }
            write(batch, List(ert))

        }
      }
      if (writers.isEmpty) {
        context.parent ! ("batch", batch, events.size)
        context.parent ! events.size
      }
      if (removed > 0) {
        logger.debug(s"filter data:$removed")
        context.parent ! ("batch", batch, removed)
      }
      if (logger.isDebugEnabled) {
        logger.debug(s"concurrence:$LEXER_SIZE;per parse [size:${events.size - removed}],extends[${count}],take ${System.currentTimeMillis() - now} Millis]")
        count = 0

      }
      if (cpu_lower) {
        TimeUnit.MILLISECONDS.sleep(1)
      }
      sender() ! Opt.GET

    /*case events: List[F] =>
      events.foreach { event =>
        sender() ! parse(event)
      }
      context.parent ! Opt.GET*/

    case obj =>
      logger.warn(s"unknown msg $obj")
  }
}
