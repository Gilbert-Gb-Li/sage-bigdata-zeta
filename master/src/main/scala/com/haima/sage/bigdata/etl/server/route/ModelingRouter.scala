package com.haima.sage.bigdata.etl.server.route

import java.util.{Date, UUID}
import javax.ws.rs.Path

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.ModelingServer
import io.swagger.annotations._

import scala.concurrent.Future
import scala.util.{Failure, Success}


/**
  * Created: 2016-05-13 14:00.
  * Author:zhhuiyan
  * Created: 2016-05-13 14:00.
  *
  *
  */
@Api(value = "数据建模使用", produces = "application/json")
@Path("/Modeling/")
class ModelingRouter(override val context:akka.actor.ActorSystem) extends DefaultRouter{
  private lazy val server = context.actorOf(Props[ModelingServer])
  @ApiOperation(value = "获取分页的数据建模信息", notes = "当start为空时，返回所有数据建模信息list[]", httpMethod = "get")
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
    new ApiResponse(code = 200, message = "分页的数据建模列表", response = classOf[Pagination[ModelingWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list:Route = pathEndOrSingleSlash{
      parameters('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?, 'pretty.?) {
        (start: Option[String], limit: Option[String], orderBy: Option[String],
         order: Option[String], search: Option[String], pretty: Option[String]) =>
          start match {
            case Some(data) if data.matches("\\d+") =>
              onComplete[Pagination[ModelingWrapper]] {
                (server ? (data.toInt, limit.getOrElse("10").toInt, orderBy, order, search.map(mapper.readValue[ModelingWrapper]))).asInstanceOf[Future[Pagination[ModelingWrapper]]]
              } {
                case Success(value) => complete(toJson(value, pretty))
                case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
                //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
              }
            case None =>
              onComplete[List[ModelingWrapper]]((server ? Opt.GET).asInstanceOf[Future[List[ModelingWrapper]]]) {
                case Success(value) => complete(toJson(value, pretty))
                case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
                //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

              }
          }
      }
  }

  @ApiOperation(value = "获取数据建模",notes="根据ID获取数据建模",httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "模型配置", response = classOf[ModelingWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
  private def byId:Route = path(Segment){
    id =>
      parameter('pretty.?) {
        pretty =>
          onComplete[Option[Any]]((server ? (Opt.GET, id)).asInstanceOf[Future[Option[Any]]]) {
            case Success(data) =>
              complete(toJson(data, pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
            //complete(toJson(Result(StatusCodes.InternalServerError.intValue.toString(), s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin), pretty))
          }
      }
  }

  @ApiOperation(value = "检查模型合法性", notes = "检查模型合法性", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[ModelingWrapper], paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "模型列表", response = classOf[List[ModelingWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("check")
  private def preCheck:Route = path("check"){
    entity(as[String]) {
      json =>
        parameter('pretty.?) {
          pretty=>
          logger.info(s"check url: $json")
          val data: Map[String, String] = mapper.readValue[Map[String, String]](json)
          val workerId = data.get("workerId")
          val url = data.get("url")
          if (workerId.isEmpty) {
            complete(toJson(BuildResult(StatusCodes.BadRequest.intValue.toString, "928", "采集器不能为空"), pretty))
          } else if (url.isEmpty) {
            complete(toJson(BuildResult(StatusCodes.BadRequest.intValue.toString, "928", "Flink地址不能为空"), pretty))
          } else {
            onComplete[BuildResult] {
              (server ? (Opt.CHECK, data.get("workerId").get, data.get("url").get)).asInstanceOf[Future[BuildResult]]
            } {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(toJson(Result(StatusCodes.InternalServerError.intValue.toString(), s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin), pretty))
            }
          }
        }

    }
  }

  @ApiOperation(value = "新增或者更新", notes = "新增或者更新模型", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[ModelingWrapper], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[Result]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("")
  private def addOrUpdate:Route = entity(as[String]){
    json =>
      parameter('pretty.?) {
        pretty=>
        entity(as[String]) { json =>
          logger.info("receive [create a ModelingWrapper type] json:{}", json)
          val wrapper: ModelingWrapper = mapper.readValue[ModelingWrapper](json)
          onComplete[BuildResult]((server ? {
            wrapper.id match {
              case Some(id) =>
                (Opt.SYNC, (Opt.UPDATE, wrapper))
              case None =>
                (Opt.SYNC, (Opt.CREATE, wrapper.copy(
                  id = Some(UUID.randomUUID().toString),
                  status = Status.PENDING,
                  lasttime = Some(new Date()),
                  createtime = Some(new Date())))
                )
            }
          }).asInstanceOf[Future[BuildResult]]) {
            case Success(result) =>
              complete(toJson(result,pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
          }
        }
      }
  }

  @ApiOperation(value = "启动/停止", notes = "启动/停止数据建模", httpMethod = "put")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "opt", value = "消息体", required = true,
      dataTypeClass = classOf[Opt.Opt], paramType = "path"),
    new ApiImplicitParam(name = "id", value = "消息体", required = true,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[Result]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("{opt}/{id}")
  private def startOrStop: Route = path(Segment / Segment) {
    (opt, id) => {
      parameter('pretty.?) {
        pretty =>
          onComplete[BuildResult] {

            logger.debug(s"receive $opt $id")
            (server ? (Opt.withName(opt.toUpperCase()), id)).asInstanceOf[Future[BuildResult]]
          } {
            case Success(value) => complete(toJson(value, pretty))
            case Failure(ex) =>
              val time = Constants.CONF.getString("master.timeout")
              val hostname = Constants.CONF.getString("master.web.http.host")
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))
            //complete(StatusCodes.InternalServerError, s"Connect to server[$hostname] has timed out[$time], please try again later.")
          }
      }
    }
  }

  @Path("type/{Segment}")
  @ApiOperation(value = "显示分类", notes = "显示分类", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "type", value = "类型", required = true,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "建模的数据列表", response = classOf[Pagination[ModelingWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def typeList:Route = path("type"/Segment){
    (`type`) =>
      parameter('pretty.?){
        pretty =>
          onComplete[List[ModelingWrapper]] {
            (server ? (Opt.GET, AnalyzerType.withName(`type`))).asInstanceOf[Future[List[ModelingWrapper]]]
          } {
            case Success(value) => complete(toJson(value, pretty))
            case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
            //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
          }
      }
  }

  @ApiOperation(value = "删除", notes = "根据ID删除数据建模", httpMethod = "delete")
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
            onComplete[BuildResult]((server ? (Opt.SYNC, (Opt.DELETE, id))).asInstanceOf[Future[BuildResult]]) {
              case Success(data) =>
                complete(toJson(data, pretty))
              case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"926",ex.getMessage),pretty))
            }
        }
    }
  }

  def route: Route = {
    pathPrefix("modeling") {
      get {
        list ~ byId ~ typeList
      } ~
        put{
          startOrStop
        } ~
        // create a datasource
        post {
          preCheck ~ addOrUpdate
        } ~
        _delete

    }
  }

}

//trait ModelingRouter extends DefaultRouter {
//  private lazy val server = context.actorOf(Props[ModelingServer])
//  val modelingHandle = {
//    pathPrefix("modeling") {
//      parameters('pretty.?) { pretty =>
//        pathEnd {
//          get {
//            parameters('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?) {
//              (start, limit, orderBy, order, search) =>
//                start match {
//                  case Some(data) if data.matches("\\d+") =>
//                    onComplete[Pagination[ModelingWrapper]] {
//                      (server ? (data.toInt, limit.getOrElse("10").toInt, orderBy, order, search.map(mapper.readValue[ModelingWrapper]))).asInstanceOf[Future[Pagination[ModelingWrapper]]]
//                    } {
//                      case Success(value) => complete(toJson(value, pretty))
//                      case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                        //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//                    }
//                  case None =>
//                    onComplete[List[ModelingWrapper]]((server ? Opt.GET).asInstanceOf[Future[List[ModelingWrapper]]]) {
//                      case Success(value) => complete(toJson(value, pretty))
//                      case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                        //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                    }
//
//                }
//            }
//          } ~
//            post {
//              requestEncodedWith(HttpEncoding.custom("UTF-8")) {
//                entity(as[String]) { json =>
//                  logger.info("receive [create a ModelingWrapper type] json:{}", json)
//                  val wrapper: ModelingWrapper = mapper.readValue[ModelingWrapper](json)
//                  onComplete[BuildResult]((server ? {
//                    wrapper.id match {
//                      case Some(id) =>
//                        (Opt.SYNC, (Opt.UPDATE, wrapper))
//                      case None =>
//                        (Opt.SYNC, (Opt.CREATE, wrapper.copy(
//                          id = Some(UUID.randomUUID().toString),
//                          status = Status.PENDING,
//                          lasttime = Some(new Date()),
//                          createtime = Some(new Date())))
//                      )
//                    }
//                  }).asInstanceOf[Future[BuildResult]]) {
//                    case Success(result) =>
//                      complete(toJson(result,pretty))
//                    case Failure(ex) =>
//                      complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                  }
//                }
//              }
//            }
//        }~
//          path("type" / Segment) {
//                (`type`) =>
//                  onComplete[List[ModelingWrapper]] {
//                    (server ? (Opt.GET, AnalyzerType.withName(`type`))).asInstanceOf[Future[List[ModelingWrapper]]]
//                  } {
//                    case Success(value) => complete(toJson(value, pretty))
//                    case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                    //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//                  }
//          }~
//          path("check") {
//            // 操作标示/ workerID / Flink web url
//            post {
//              requestEncodedWith(HttpEncoding.custom("UTF-8")) {
//                entity(as[String]) { json =>
//                  logger.info(s"check url: $json")
//                  val data: Map[String, String] = mapper.readValue[Map[String, String]](json)
//                  val workerId = data.get("workerId")
//                  val url = data.get("url")
//                  if (workerId.isEmpty) {
//                    complete(toJson(BuildResult(StatusCodes.BadRequest.intValue.toString,"928","采集器不能为空"),pretty))
//                  } else if (url.isEmpty){
//                    complete(toJson(BuildResult(StatusCodes.BadRequest.intValue.toString,"928","Flink地址不能为空"),pretty))
//                  }else {
//                    onComplete[BuildResult] {
//                      (server ? (Opt.CHECK, data.get("workerId").get, data.get("url").get)).asInstanceOf[Future[BuildResult]]
//                    } {
//                      case Success(value) => complete(toJson(value, pretty))
//                      case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                      //complete(toJson(Result(StatusCodes.InternalServerError.intValue.toString(), s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin), pretty))
//                    }
//                  }
//                }
//              }
//            }
//          } ~
//          path(Segment) { id =>
//
//            get {
//              onComplete[Option[Any]]((server ? (Opt.GET, id)).asInstanceOf[Future[Option[Any]]]) {
//                case Success(data) =>
//                  complete(toJson(data, pretty))
//                case Failure(ex) =>
//                  complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                  //complete(toJson(Result(StatusCodes.InternalServerError.intValue.toString(), s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin), pretty))
//              }
//            } ~
//              delete {
//                onComplete[Boolean]((server ? (Opt.SYNC, (Opt.DELETE, id))).asInstanceOf[Future[Boolean]]) {
//                  case Success(data) =>
//                    val rt = if (data)
//                      BuildResult(StatusCodes.OK.intValue.toString,"1102")
//                      //Result(StatusCodes.OK.intValue.toString, s"DELETE HANDLE[$id] SUCCESS")
//                    else
//                      BuildResult(StatusCodes.InternalServerError.intValue.toString,"1103")
//                      //Result(StatusCodes.InternalServerError.intValue.toString, s"DELETE HANDLE[$id] FAILED")
//                    complete(toJson(rt, pretty))
//                  case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                    //complete(toJson(Result(StatusCodes.InternalServerError.intValue.toString(), s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin), pretty))
//                }
//              }
//
//          } ~
//          path(Segment / Segment) {
//            // 操作标示/ ID标示
//            (opt, id) => {
//              put {
//                onComplete[BuildResult] {
//                  (server ? (Opt.withName(opt.toUpperCase()), id)).asInstanceOf[Future[BuildResult]]
//                } {
//                  case Success(value) => complete(toJson(value, pretty))
//                  case Failure(ex) => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                    //complete(toJson(Result(StatusCodes.InternalServerError.intValue.toString(), s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin), pretty))
//                }
//              }
//            }
//          }
//      }
//    }
//  }
//
//}
