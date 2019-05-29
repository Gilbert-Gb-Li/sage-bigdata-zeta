package com.haima.sage.bigdata.etl.plugin.flink

import java.util
import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.{ByteString, Timeout}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.haima.sage.bigdata.etl.common.model.{FlinkAPIs, Opt, Status}
import com.haima.sage.bigdata.etl.plugin.flink.client.http.FlinkHttpClient
import com.haima.sage.bigdata.etl.utils.Logger

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by evan on 17-10-26.
  */
class FlinkWatcher(address: String) extends Actor with Logger {
  implicit val timeout = Timeout(20 seconds)

  import context.dispatcher

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
  private val OVERVIEW = "overview"
  private val JOBS = "jobs"
  private val JOB_MANAGER_CONFIG = "jobmanager/config"
  private val JOB_CONCELLATION = "yarn-cancel"

  private var client: ActorRef = _

  override def preStart(): Unit = {
    super.preStart()
    client = context.actorOf(Props.create(classOf[FlinkHttpClient])
      .withDispatcher("akka.dispatcher.executor"), s"flink-httpclient-${UUID.randomUUID()}")
  }


  override def receive: Receive = {
    case FlinkAPIs.CLUSTER_OVERVIEW =>
      execute(s"$address/$OVERVIEW")(sender())
    case FlinkAPIs.SLOTS_AVAILABLE =>
      val actor = sender()
      exec[Int, Map[String, String]](s"$address/$OVERVIEW")({ body =>
        import scala.collection.JavaConversions._
        val value = JSON.parseObject(body.utf8String).toJavaObject(classOf[util.Map[String, Object]]).toMap
        value.get("slots-available").map(_.asInstanceOf[Int]).getOrElse(0)
      })(actor)

    case FlinkAPIs.JOBS_OVERVIEW =>

      execute(s"$address/$JOBS")(sender())
    case (jobId: String, FlinkAPIs.JOB_DETAILS) =>

      execute(s"$address/$JOBS/$jobId")(sender())
    case (jobId: String, FlinkAPIs.JOB_METRICS) =>

      implicit val actor: ActorRef = sender()

      exec[Map[String, Long], Map[String, String]](s"$address/$jobId")({ body =>
        import scala.collection.JavaConversions._
        val value = JSON.parseObject(body.utf8String).toJavaObject(classOf[util.Map[String, Object]]).toMap
        val metrics: List[Map[String, Long]] = value.get("vertices") match {
          case Some(data) =>
            import scala.collection.JavaConversions._
            data.asInstanceOf[JSONArray]
              .toList
              .map {
                info =>
                  val metricsData = info.asInstanceOf[JSONObject].get("metrics")
                  if (metricsData != null) {
                    metricsData.asInstanceOf[JSONObject]
                      .toJavaObject(classOf[util.Map[String, Any]])
                      .toMap
                      .map { e =>
                        e._2 match {
                          case l: Long => e._1 -> l
                          case i: Int => e._1 -> i.toLong
                          case _ => e._1 -> 0L
                        }
                      }
                  } else {
                    Map[String, Long]()
                  }
              }.filter(_.nonEmpty)
          case None =>
            List()
        }
        var result: mutable.Map[String, Long] = mutable.Map()
        metrics.foreach {
          data =>
            if (result.isEmpty) result = result.++(data)
            else data.foreach {
              d =>
                result = result.+(d._1 -> {
                  result.get(d._1) match {
                    case Some(v1) =>
                      v1 + d._2
                    case None =>
                      d._2
                  }
                })
            }
        }
        result.toMap
      })(actor)
    case (jobId: String, FlinkAPIs.JOB_STATUS) =>
      val actor = sender()
      status(jobId, actor)
    case FlinkAPIs.RPC_ADDRESS =>
      implicit val actor: ActorRef = sender()

      exec[Option[FlinkAddress], Map[String, String]](s"$address/$JOB_MANAGER_CONFIG")({ body =>
        import scala.collection.JavaConversions._
        val jobManagerConfig = JSON.parseArray(body.utf8String).toList.map(
          data =>
            (data.asInstanceOf[JSONObject].getString("key"), data.asInstanceOf[JSONObject].getString("value"))
        ).toMap

        (jobManagerConfig.get("jobmanager.rpc.address"), jobManagerConfig.get("jobmanager.rpc.port")) match {
          case (Some(addre), Some(port)) =>
            val proxy = address.split("/proxy/").toList match {
              case head :: second :: Nil if second.length > 0 =>
                Some(second)
              case _ =>
                None
            }
            Some(FlinkAddress(addre, port.toInt, proxy))
          case _ =>
            None
        }

      })(actor)
    case FlinkAPIs.JOB_MANAGER_CONFIG =>
      implicit val actor: ActorRef = sender()

      exec[Map[String, String], Map[String, String]](s"$address/$JOB_MANAGER_CONFIG")({ body =>
        import scala.collection.JavaConversions._
        JSON.parseArray(body.utf8String).toList.map(
          data =>
            (data.asInstanceOf[JSONObject].getString("key"), data.asInstanceOf[JSONObject].getString("value"))
        ).toMap
      })(actor)
    case (jobId: String, FlinkAPIs.JOB_CONCELLATION) =>
      execute(s"$address/$JOBS/$jobId/$JOB_CONCELLATION")
    case Opt.STOP =>
      logger.info(s"stop flink watcher[${self.path.name}] from ${sender().path.name}")
      client ! Opt.STOP
      context.stop(self)
    case msg =>
      logger.warn(s"unknown message[$msg] from ${sender().path.name} to [${self.path.name}]")
  }

  //获取任务运行状态
  def status(jobId: String, actor: ActorRef): Unit = {
    exec[Status.Status, Status.Status](s"$address/$JOBS/$jobId")({ body =>
      import scala.collection.JavaConversions._
      val rt: Status.Status = JSON.parseObject(body.utf8String).toJavaObject(classOf[util.Map[String, Object]]).toMap.
        get("state") match {
        case Some(value) =>
          // logger.debug(s"Flink job[$jobId] status is $value .")
          value match {
            case "CREATED" =>
              Status.RUNNING
            case "FINISHED" =>
              Status.FINISHED
            case "RUNNING" =>
              Status.RUNNING
            case "CANCELED" =>
              Status.STOPPED
            case "FAILED" =>
              Status.FAIL
            case v =>

              logger.warn(s"status is :$v")
              Status.ERROR
          }
        case None =>
          Status.UNKNOWN
      }
      rt
    }, {
      case StatusCodes.NotFound =>
        Status.UNKNOWN
      case v =>
        logger.warn(s"status:$v")
        Status.ERROR
    }, {
      e =>
        logger.warn(s"ex:$e")
        Status.UNAVAILABLE
    })(actor)
  }


  def exec[T, E](path: String)(func: ByteString => T = {
    body: ByteString =>
      import scala.collection.JavaConversions._
      JSON.parseObject(body.utf8String).toJavaObject(classOf[util.Map[String, Object]]).toMap.asInstanceOf[T]
  }, CFunc: (StatusCode) => E = {
    c: StatusCode =>
      Map("error" -> s"connect flink error, response code: $c").asInstanceOf[E]
  }
                               , EFunc: (Exception) => E = {
    ex: Exception =>
      Map("error" -> s"connect flink error, ${ex.getMessage}").asInstanceOf[E]
  })(actorRef: ActorRef): Unit = {
    try {
      Await.result(client ? buildGetRequest(Uri(path)), timeout.duration).asInstanceOf[HttpResponse] match {
        case HttpResponse(StatusCodes.OK, headers, entity, _) =>
          entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
            body =>
              actorRef ! func(body)
          }
        case resp@HttpResponse(code, _, _, _) =>
          resp.discardEntityBytes()

          logger.debug(s"status code:" + code)
          actorRef ! CFunc(code)
      }
    } catch {
      case ex: Exception =>
        logger.error("FlinkWatcher execute error ", ex)
        actorRef ! EFunc(ex)
    }
  }

  def execute(path: String)(implicit actorRef: ActorRef): Unit = {
    exec[Map[String, Any], Map[String, Any]](path)()(actorRef)
  }

  def buildGetRequest(uri: Uri): HttpRequest = HttpRequest(uri = uri, method = HttpMethods.GET, entity = HttpEntity.empty(ContentTypes.`application/json`))
}

case class FlinkAddress(host: String, port: Int, application: Option[String] = None)