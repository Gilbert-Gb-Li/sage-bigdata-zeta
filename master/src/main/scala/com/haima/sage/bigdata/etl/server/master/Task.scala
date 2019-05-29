package com.haima.sage.bigdata.etl.server.master

import java.text.SimpleDateFormat
import java.util.Calendar

import akka.actor._
import akka.http.scaladsl.model.HttpRequest
import akka.stream.scaladsl.Framing
import akka.util.ByteString
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.Opt
import com.haima.sage.bigdata.etl.server.Master
import com.haima.sage.bigdata.etl.store.Stores
import com.haima.sage.bigdata.etl.utils.{Dictionary, Logger, Mapper, TimeUtils}


import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2015/3/24.
  */
class Task extends akka.actor.Actor with Logger with Mapper {

  val system = context.system
  import  context.dispatcher
  import Master.materializer

  // 定时删除metrics信息 Task
  private def deleteMetricsTask(): Unit = {
    // 设定 删除的起止时间 默认为前一天0时到第二天0时
    def duration(): (String, String) = {
      val calendar = Calendar.getInstance()
      val to = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime) + " 00:00:00"
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      val from = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime) + " 00:00:00"
      (from, to)
    }

    // 设定初始启动等待时长， 默认开始删除时段为00:00:00~05:00:00
    def initialDelay(): Long = {
      val calendar = Calendar.getInstance()
      val clone = calendar.clone().asInstanceOf[Calendar]
      clone.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 23, 59, 59)
      val dv = TimeUtils.dateCompare(clone.getTime, calendar.getTime) / 1000
      if (dv > 19 * 60 * 60 || dv <= 1) 1
      else dv
    }

    import scala.concurrent.duration._
    val _initialDelay = initialDelay()
    logger.debug("initialDelay = " + _initialDelay)
    // 每天执行一次
    system.scheduler.schedule(_initialDelay seconds, 1 day) {
      val _duration = duration()
      try {
        Stores.metricInfoStore.delete(_duration._1, _duration._2)
      } catch {
        case ex: Exception => logger.error("Delete metrics info failed: ", ex)
      }
    }
  }


  private def loadDictionary(): Unit = {
    def execute(httpRequest: HttpRequest) = {
      logger.info(httpRequest.uri.toString())

      val future = Master.web.singleRequest(httpRequest)

      future.onComplete {
        case Success(msg) =>
          logger.info(s"$msg")
        case Failure(fail) =>
          logger.error(s"$fail")
      }
      future
    }

    val remote = Constants.MASTER.getConfig("dictionary")

    val http = s"http://${remote.getString("host")}:${remote.getString("port")}${remote.getString("path")}"
    import akka.http.scaladsl.model.HttpMethods._
    val json = execute(HttpRequest(GET, uri = http)).onComplete {
      case Success(data) =>
        data.entity.dataBytes.via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024)).runForeach(json => {
          val fields = mapper.readValue[Map[String, Map[String, Any]]](json.asByteBuffer.array())


          val fill_fields = fields.get("fields") match {
            case Some(d: List[String]@unchecked) =>
              d.filter(_ != "").toSet[String]
            case _ =>
              Set[String]()
          }
          val datas: Map[String, AnyRef]@unchecked = fields.get("datas") match {
            case Some(d: Map[String, Any]@unchecked) =>
              d.values.map {
                case real: Map[String, Any]@unchecked =>

                  (real.get("data") match {
                    case Some(elements: List[Any]@unchecked) =>
                      elements.map {
                        case kv: Map[String, Any]@unchecked =>
                          kv.map {
                            case (key, (value)) =>
                              (key, value.toString())
                          }
                        case _ =>
                          Map[String, String]()
                      }.toList
                    case _ =>
                      List.empty
                  }) match {
                    case Nil =>
                      ("", null)
                    case data =>
                      real.get("entity") match {
                        case Some(("asset")) =>
                          ("asset", data.map(obj => (obj.getOrElse("asset_id", ""), obj)).toMap ++ data.map(obj => (obj.getOrElse("asset_ip", ""), obj)).toMap)
                        case Some(("person")) =>
                          ("person", data.map(obj =>
                            obj.getOrElse("person_ip", "").split(",").map((_, obj)).toMap
                          ).reduce((first, second) => first ++ second))
                        case Some(("location")) =>
                          ("location", data.sortBy(entity => {
                            entity.getOrElse("begin_ip_number", "-1").toLong - entity.getOrElse("end_ip_number", "0").toLong
                          }))
                        case Some(("log_rule")) =>
                          ("rule", data.map(obj => (obj.getOrElse("log_rule_id", ""), obj)).toMap)
                        case _ =>
                          ("", null)
                      }
                  }
                case _ =>
                  ("", null)
              }.toMap[String, AnyRef]
            case _ =>
              Map()
          }
          val location = datas.getOrElse("location", List()).asInstanceOf[List[Map[String, String]]]
          val rules = datas.getOrElse("rule", Map()).asInstanceOf[Map[String, Map[String, String]]]
          val asset = datas.getOrElse("asset", Map()).asInstanceOf[Map[String, Map[String, String]]]
          val person = datas.getOrElse("person", Map()).asInstanceOf[Map[String, Map[String, String]]]
          /* TODO Master.workers.values.foreach(worker => {
            worker ! Dictionary(scala.collection.mutable.LinkedHashMap(asset.toList: _*), person, location, rules, fill_fields)
          })*/

        })


      case Failure(e) =>
        logger.error("get dictionary faild : ", e)
    }
  }

  /*
    获取数据字段
   */
  private def dictionary(): Unit = {
    import scala.concurrent.duration._
    system.scheduler.schedule(0 seconds, 1 minute) {

      /* TODO if (Master.workers.size > 0) {
        loadDictionary()
      }*/

    }
  }

  def taskWorker(path: String, id: String): ActorRef = context.actorOf(Props.create(classOf[TaskWorker], path, id))

  private val taskWorkerOptler = scala.collection.mutable.HashMap[String, ActorRef]()

  deleteMetricsTask()

  //TODO load Data dictionary()

  override def receive: Receive = {
    case "loadDictionary" =>
      loadDictionary()
    case (Opt.START, path: String, id: String) =>
      val worker = taskWorker(path, id)
      taskWorkerOptler.+=(path -> worker)
      logger.info(s"Start task worker : $worker")

    case (Opt.STOP, path: String) =>
      taskWorkerOptler.get(path) match {
        case Some(actor) =>
          logger.info(s"Stop task worker : $actor")
          taskWorkerOptler - path
          actor ! Opt.STOP
        case None =>
          logger.warn(s" no worker monitor find !!!")
      }
  }
}

class TaskWorker(path: String, id: String) extends akka.actor.Actor with Logger {
  import  context.dispatcher

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    context.actorSelection(path) ! Identify(id)
  }

  override def receive: Receive = {
    case ActorIdentity(`id`, Some(actor)) =>
      logger.info(s"Receive the actorRef of worker, $actor .")

      context.actorSelection("/user/watcher") ! (Opt.WATCH, actor)
      context.stop(self)
    case ActorIdentity(`id`, None) =>
      import scala.concurrent.duration._

      logger.error(s"remote[$path] worker[$id] mabe not running ,retry")
      context.system.scheduler.scheduleOnce(10 seconds) {
        context.actorSelection(path) ! Identify(id)
      }

    case Opt.STOP =>

      context.stop(self)

  }

}
