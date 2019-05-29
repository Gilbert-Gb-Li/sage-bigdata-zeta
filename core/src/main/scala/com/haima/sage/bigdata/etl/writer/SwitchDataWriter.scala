package com.haima.sage.bigdata.etl.writer

import java.util.concurrent.atomic.AtomicLong

import akka.actor.Props
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.exception.LogWriteException
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.metrics.MeterReport

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */

class SwitchDataWriter(conf: SwitchWriter, timer: MeterReport) extends DefaultWriter[SwitchWriter](conf, timer) with BatchProcess {


  val writers = conf.writers.zipWithIndex.map {
    case ((v, wc), index) =>


      val path =s"""${wc.name}_$index""".replaceAll("[/\\\\]", ".")
      logger.debug(s"writer[$wc] ${v} to $path")
      val clazz = CONF.getConfig("app.writer").getString(wc.name)
      (v, context.actorOf(Props.create(Class.forName(clazz), wc, timer)
        .withDispatcher("akka.dispatcher.executor"), path))
  }.toList

  val default_name =s"""${conf.default.name}_default""".replaceAll("[/\\\\]", ".")
  val default = context.actorOf(Props.create(Class.forName(CONF.getConfig("app.writer").getString(conf.default.name)), conf.default, timer)
    .withDispatcher("akka.dispatcher.executor"), default_name)
  logger.debug(s"writer[${conf.default}] default to $default_name")

  private val unFinishedBatches: mutable.Map[Long, (Long, Int)] = mutable.Map()
  private val index = new AtomicLong(0)


  /**
    * type 操作
    * _1 数据值,_2 匹配值
    *
  * */
  lazy val  func: (String, String) => Boolean = conf.`type` match {
    case SwitchType.Match =>
      (f: String, s: String) => {
        f.equals(s)
      }
    case SwitchType.StartWith =>
      (f: String, s: String) => f.startsWith(s)
    case SwitchType.EndWith =>
      (f: String, s: String) => f.endsWith(s)
    case SwitchType.Contain =>
      (f: String, s: String) => f.contains(s)
    case SwitchType.Regex =>
      (f: String, s: String) => f.matches(s)
  }

  @throws(classOf[LogWriteException])
  override def write(batch: Long, t: List[RichMap]): Unit = {
    process(batch)
    val processors = t.groupBy(data => {
      data.get(conf.field) match {
        case Some(d) if d != null =>
          writers.find(t => func(d.toString, t._1)) match {
            case Some((_, actor)) =>

              logger.debug("actor:"+actor.toString())
              actor
            case _ =>
              logger.debug("default:"+default.toString())
              default
          }
        case _ =>
          logger.debug("default2:"+default.toString())
          default

      }
    })


    unFinishedBatches.put(index.get(), batch -> processors.size)
    processors.foreach(process => process._1 ! ((index.get(), process._2)))

    /*t.foreach(data => {
      data.get(conf.field) match {
        case Some(d) if d != null =>
          val real = writers.filter {
            case (value, _) =>
              conf.`type` match {
                case SwitchType.Match =>
                  value.equals(d.toString)
                case SwitchType.StartWith =>
                  d.toString.startsWith(value)
                case SwitchType.EndWith =>
                  d.toString.endsWith(value)
                case SwitchType.Contain =>
                  d.toString.contains(value)
                case SwitchType.Regex =>
                  d.toString.matches(value)
              }

          }
          if (real.isEmpty) {
            default forward data
          } else {
            real.head._2 forward data
          }


        case _ =>
          default forward data

      }
    })*/

  }


  var counter = 0

  override def close(): Unit = {
    writers.foreach(_._2 ! STOP)
    synchronized {
      if (context.children.nonEmpty && counter < 60) {
        context.children.foreach(_ ! STOP)
        counter += 1
        logger.debug(s"writer waiting 1 seconds until data has been store to elasticsearch ")
        context.system.scheduler.scheduleOnce(1 seconds, self, STOP)
      } else {
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
        context.stop(self)
      }
    }
  }

  override def flush(): Unit = {
    writers.foreach(_._2 ! FLUSH)
  }

  var runners: Map[String, Status.Status] = writers.map {
    case (_, actor) =>
      (actor.path.toString, Status.RUNNING)
  }.toMap


  override def receive: Receive = {
    case (_: String, batches: Map[Long@unchecked, Int@unchecked]) =>
      batches.foreach {
        case (batch, number) =>
          unFinishedBatches.get(batch) match {
            case Some((b, n)) =>
              if (n - 1 == 0) {
                unFinishedBatches.remove(batch)
              } else {
                unFinishedBatches.put(batch, (b, n - 1))
              }
            case _ =>
          }

      }

      if (unFinishedBatches.isEmpty) {
        report()
      }
    case (_: String, ProcessModel.WRITER, Status.RUNNING) =>
      runners = runners + (sender().path.toString -> Status.RUNNING)
      if (runners.forall(_._2 == Status.RUNNING)) {
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.RUNNING)
      }

    case (_: String, ProcessModel.WRITER, Status.PENDING, "CONNECTING") =>
      runners = runners + (sender().path.toString -> Status.PENDING)
      context.parent ! (self.path.name, ProcessModel.WRITER, Status.PENDING, "CONNECTING")
    case (_: String, ProcessModel.WRITER, Status.ERROR, "LOST_CONNECTED") =>
      runners = runners + (sender().path.toString -> Status.ERROR)
      context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, "LOST_CONNECTED")
    case (_: String, ProcessModel.WRITER, Status.STOPPED) =>
      runners = runners + (sender().path.toString -> Status.STOPPED)
      if (runners.forall(_._2 == Status.STOPPED)) {
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
        context.stop(self)
      }
    case obj =>
      super.receive(obj)
  }

  override def write(t: RichMap): Unit = ???
}