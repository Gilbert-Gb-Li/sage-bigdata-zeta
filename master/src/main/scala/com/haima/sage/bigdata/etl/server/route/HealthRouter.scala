package com.haima.sage.bigdata.etl.server.route

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store.Stores

import scala.concurrent._
import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2015/6/18.
  */
trait HealthRouter extends DefaultRouter {

  import context.dispatcher

  // 健康信息监控 URI:health/<collectorId>/<configId>
  val health = {
    pathPrefix("health") {
      path(Segments) {
        ids =>
          parameter('hType.?, 'host.?, 'port.?, 'pretty.?) {
            (hType, host, port, pretty) =>
              get {

                onComplete[AnyRef](if (ids.length == 2) {
                  load(ids.head, Some(ids(1)), host, port, hType)
                } else {
                  load(ids.head, None, host, port, hType)
                }) {
                  case Success(data) =>
                    complete(toJson(data, pretty))
                  case Failure(ex) =>
                    complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
                    //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

                }
              }
          }

      }
    }
  }


  def load(collectorId: String, configId: Option[String] = None, host: Option[String], port: Option[String], hType: Option[String] = None): Future[AnyRef] = {
    try {
      val c = (host, port) match {
        case (Some(h: String), Some(p: String)) =>
          Collector(collectorId, h, p.toInt)
        case _ =>
          val data = collectorId.split(":")
          if (data.length == 2) {
            Collector(null, data(0), data(1).toInt)
          } else {
            Collector(collectorId, null, 0)
          }
      }

      Stores.collectorStore.get(c) match {
        case Some(collector) =>

          val remoteActor = context.actorSelection("/user/server")

          hType match {
            case Some("collector") =>
              (remoteActor ? (collector.id, Opt.CHECK)).map {
                case status: Status.Status =>
                  status
                case rt: Result =>
                  Status.UNAVAILABLE
              }.map(status => Map("collectorStatus" -> status))

            case Some("datasource") =>
              val future = configId match {
                case Some(id) =>
                  Stores.configStore.get(id) match {
                    case Some(info) =>
                      remoteActor ? (collector.id, (Opt.GET, id))

                    case None =>
                      remoteActor ? (collector.id, Opt.GET)
                  }
                case None =>
                  remoteActor ? (collector.id, Opt.GET)
              }

              future.map {
                case data: List[RunStatus@unchecked] =>
                  dataSourceStatus(data)
                case rt: Result =>
                  Map("collectorStatus" -> Status.UNAVAILABLE)
              }
            case None =>

              val future = configId match {
                case Some(id) =>
                  Stores.configStore.get(id) match {
                    case Some(info) =>
                      remoteActor ? (collector.id, (Opt.GET, id))
                    case None =>
                      remoteActor ? (collector.id, Opt.GET)
                  }
                case None =>
                  remoteActor ? (collector.id, Opt.GET)
              }

              future.map {
                case data: List[RunStatus@unchecked] =>
                  dataSourceStatus(data)
                case rt: Result =>
                  Map("collectorStatus" -> Status.UNAVAILABLE)
              }


            case Some(other: String) =>
              Future {
                Result(StatusCodes.BadRequest.intValue.toString, s"${StatusCodes.BadRequest.reason} : [$other] NOT ACCEPTABLE")
              }
          }
        case None =>
          Future(Result(StatusCodes.NotFound.intValue.toString, s"${StatusCodes.NotFound.reason} : COLLECTOR[$collectorId] NOT FOUND"))
      }
    } catch {
      case ex: TimeoutException =>
        logger.error("GET HEALTH INFO ERROR", ex)
        //                    Result(StatusCodes.RequestTimeout.intValue.toString, s"${StatusCodes.RequestTimeout.reason} : REQUEST COLLECTOR[${serverInfo.id} / (${serverInfo.ip}:${serverInfo.port})] TIMEOUT").toJson.toString()
        Future(Map(
          "collectorStatus" -> "UNAVAILABLE",
          "reason" -> s"THE COLLECTOR[$collectorId($host:$port)] REQUEST TIMEOUT. IT MAY BE STOPPED"
        ))
      case ex: Exception =>
        logger.error("GET HEALTH INFO ERROR", ex)
        Future(Result(StatusCodes.InternalServerError.intValue.toString, s"API SERVICE ERROR : ${ex.getMessage}"))
    }
  }


  private def dataSourceStatus(data: List[RunStatus]): Map[String, Map[String, Object]] = {
    def toJson(data: List[RunStatus]) = {
      var monitorStatus: String = null
      var streamStatus = Map[String, String]()
      var writerStatus = Map[String, String]()
      data.foreach(status => {
        status.model match {
          case ProcessModel.MONITOR =>
            monitorStatus = status.value.toString
          case ProcessModel.STREAM =>
            streamStatus += (status.path.get -> status.value.toString)
          case ProcessModel.WRITER =>
            writerStatus += (status.path.get -> status.value.toString)
          case msg =>
            logger.error(s"unknown model[$msg] of status")
        }

      })
      Map(
        "monitorStatus" -> monitorStatus,
        "streamStatus" -> streamStatus,
        "writerStatus" -> writerStatus
      )
    }

    data.groupBy(_.config).map(tuple => {
      (tuple._1, toJson(tuple._2))
    })
  }

  private def status(data: List[RunStatus]) = {
    data.groupBy(_.config).map(tuple => {
      (tuple._1, Map("dataSourceStatus" -> dataSourceStatus(tuple._2)))
    }) + ("collectorStatus" -> Status.RUNNING.toString)

  }
}
