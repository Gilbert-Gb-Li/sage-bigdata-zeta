package com.haima.sage.bigdata.etl.server.route

import java.util.UUID
import javax.ws.rs.Path

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.{CollectorServer}
import io.swagger.annotations._

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 15/11/25.
  */

@Api(value = "数据建模使用", produces = "application/json")
@Path("/collector/")
class CollectorRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {
  private lazy val server = context.actorOf(Props[CollectorServer])

  @ApiOperation(value = "获取分页的采集器信息", notes = "当start为空时，返回所有采集器信息list[]", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "start", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "pretty", value = "是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "limit", value = "每页显示条数", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "orderBy", value = "排序依据字段", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "order", value = "正序/反序", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "search", value = "检索条件", required = false,
      dataTypeClass = classOf[String], paramType = "form")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "分页的数据列表", response = classOf[Pagination[Collector]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list: Route = pathEndOrSingleSlash {
    parameter('start.?, 'limit.?, 'orderBy.?, 'order.?, 'pretty.?) {
      (start, limit, orderBy, order, pretty) =>
        start match {
          case Some(data) if data.matches("\\d+") =>
            onComplete[Pagination[Collector]] {
              (server ? (data.toInt, limit.getOrElse("10").toInt, orderBy, order, None)).asInstanceOf[Future[Pagination[Collector]]]
            } {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }
          case None =>
            onComplete[List[Collector]]((server ? Opt.GET).asInstanceOf[Future[List[Collector]]]) {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }

        }
    }
  }

  @ApiOperation(value = "获取采集器配置", notes = "根据ID获取采集器配置", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "通道配置", response = classOf[DataSourceWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("collect/{Segment}")
  private def byId: Route = path("collect" / Segment) {
    id =>
      parameter('pretty.?) {
        pretty=>
        pathEndOrSingleSlash {
          // get collector(id) info
          get {
            onComplete[Option[Collector]] {
              (server ? (Opt.GET, id)).asInstanceOf[Future[Option[Collector]]]
            } {
              case Success(value) =>
                value match {
                  case Some(info) =>
                    complete(toJson(info, pretty))
                  case _ =>
                    complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString, "803"), pretty))
                  //complete(toJson(Result(StatusCodes.NotFound.intValue.toString, s"Collector[$id] NOT FOUND"), pretty))
                }
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }
          }
        }
      }
  }

  @ApiOperation(value = "删除", notes = "根据ID删除数据源", httpMethod = "delete")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "开始位置", required = true,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[Result]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("{id}")
  private def _delete = delete{
    path(Segment) {
      id =>
        parameter('pretty.?) {
          pretty =>
            onComplete[Boolean] {
              (server ? (Opt.DELETE, id)).asInstanceOf[Future[Boolean]]
            } {
              case Success(value) if value =>
                complete(toJson(BuildResult("200", "804", id), pretty))
              //complete(toJson(Result("200", s"DELETE COLLECTOR[$id] OK"), pretty))
              case Success(_) =>
                complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString, "803"), pretty))
              //complete(toJson(Result(StatusCodes.NotFound.intValue.toString, s"${StatusCodes.NotFound.reason} :COLLECTOR[$id]NOT FOUND"), pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }
        }
    }

  }

  @ApiOperation(value = "操作", notes = "关闭/开启采集器", httpMethod = "put")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "action", value = "关闭/开启", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "开始位置", required = true,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[Result]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("{CollectId}/{action}")
  private def stopOrstartCollector:Route = path(Segment/Segment){
    (id,action)=>
      parameter('pretty.?){
        pretty=>
          onComplete[BuildResult]((server ? (Opt.withName(action.trim.toUpperCase), id)).asInstanceOf[Future[BuildResult]]) {
            case Success(value) =>
              logger.debug(s"opt rt:$value")
              complete(toJson(value, pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
            //complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
          }
      }

  }

  def route: Route = {
    pathPrefix("collector") {
      get {
        list ~ byId
      } ~ put{
        stopOrstartCollector
      } ~
        // create a datasource
        _delete
    }
  }


}

//trait CollectorRouter extends DefaultRouter {
//
//
//  private lazy val server =context.actorSelection("/user/collector-server")
//  val collector: Route = {
//    // URI config/collector
//
//    parameter('pretty.?) {
//
//
//      pretty => {

        // URI /collector
//        path("collector") {
//          parameter('start.?, 'limit.?, 'orderBy.?, 'order.?) {
//            (start, limit, orderBy, order) =>
//              start match {
//                case Some(data) if data.matches("\\d+") =>
//                  onComplete[Pagination[Collector]] {
//                    (server ? (data.toInt, limit.getOrElse("10").toInt, orderBy, order, None)).asInstanceOf[Future[Pagination[Collector]]]
//                  } {
//                    case Success(value) => complete(toJson(value, pretty))
//                    case Failure(ex) =>
//                      complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                    //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                  }
//                case None =>
//                  onComplete[List[Collector]]((server ? Opt.GET).asInstanceOf[Future[List[Collector]]]) {
//                    case Success(value) => complete(toJson(value, pretty))
//                    case Failure(ex) =>
//                      complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                    //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                  }
//
//              }
//          }
//        } ~ pathPrefix("collector" / Segment) {
//          id =>
//
//            pathEndOrSingleSlash {
//              // get collector(id) info
//              get {
//                onComplete[Option[Collector]] {
//                  (server ? (Opt.GET, id)).asInstanceOf[Future[Option[Collector]]]
//                } {
//                  case Success(value) =>
//                    value match {
//                      case Some(info) =>
//                        complete(toJson(info, pretty))
//                      case _ =>
//                        complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString,"803"),pretty))
//                      //complete(toJson(Result(StatusCodes.NotFound.intValue.toString, s"Collector[$id] NOT FOUND"), pretty))
//                    }
//                  case Failure(ex) =>
//                    complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                  //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                }
//              } ~ delete {
//
//                onComplete[Boolean] {
//                  (server ? (Opt.DELETE, id)).asInstanceOf[Future[Boolean]]
//                } {
//                  case Success(value) if value =>
//                    complete(toJson(BuildResult("200","804",id),pretty))
//                  //complete(toJson(Result("200", s"DELETE COLLECTOR[$id] OK"), pretty))
//                  case Success(_) =>
//                    complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString,"803"),pretty))
//                  //complete(toJson(Result(StatusCodes.NotFound.intValue.toString, s"${StatusCodes.NotFound.reason} :COLLECTOR[$id]NOT FOUND"), pretty))
//                  case Failure(ex) =>
//                    complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                  //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                }
//              }
//            } ~ path(Segment) {
//              action =>
//                onComplete[BuildResult]((server ? (Opt.withName(action.trim.toUpperCase), id)).asInstanceOf[Future[BuildResult]]) {
//                  case Success(value) =>
//                    logger.debug(s"opt rt:$value")
//                    complete(toJson(value, pretty))
//                  case Failure(ex) =>
//                    complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                  //complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
//                }
//            }
//
//        }
//      }
//    }
//  }
//}
