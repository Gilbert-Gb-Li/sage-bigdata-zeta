package com.haima.sage.bigdata.etl.server.route

import java.util.UUID
import javax.ws.rs.Path

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{Opt, _}
import com.haima.sage.bigdata.etl.server.hander.{WriteServer}
import io.swagger.annotations._

import scala.concurrent._
import scala.util.{Failure, Success}

/**
  * Created: 2016-06-02 11:39.
  * Author:zhhuiyan
  * Created: 2016-06-02 11:39.
  *
  *
  */

@Api(value = "数据存储相关", produces = "application/json")
@Path("/writer/")
class WriterRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter{
  private lazy val server = context.actorOf(Props[WriteServer])

  @ApiOperation(value = "获取数据存储信息", notes = "当start为空时，返回所有数据存储信息list[]", httpMethod = "get")
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
    new ApiResponse(code = 200, message = "分页的数据存储列表", response = classOf[Pagination[WriteWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list:Route = pathEndOrSingleSlash {
    parameters('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?,'pretty.?) {
      (start, limit, orderBy, order, search,pretty) =>
        val entry = search match {
          case Some(con: String) =>
            val data = mapper.readValue[Map[String, String]](con)
            if (data.isEmpty) {
              None
            } else {
              val name: String = data.get("name") match {
                case Some(_name: String) =>
                  _name
                case _ =>
                  null
              }
              Option(WriteWrapper(name = name, writeType = data.get("writeType"), data = null))
            }

          case _ =>
            None
        }

        start match {
          case Some(_) =>
            onComplete[Pagination[WriteWrapper]] {
              (server ? (start.getOrElse("0").toInt, limit.getOrElse("10").toInt, orderBy, order, entry)).asInstanceOf[Future[Pagination[WriteWrapper]]]
            } {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }
          case None =>
            onComplete[List[WriteWrapper]]((server ? Opt.GET).asInstanceOf[Future[List[WriteWrapper]]]) {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }

        }

    }
  }

    @ApiOperation(value = "获取数据存储",notes="根据ID获取数据存储",httpMethod = "get")
    @ApiImplicitParams(Array(
      new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
        dataTypeClass = classOf[String], paramType = "query"),
      new ApiImplicitParam(name = "id", value = "开始位置", required = false,
        dataTypeClass = classOf[String], paramType = "path")

    ))
    @ApiResponses(Array(
      new ApiResponse(code = 200, message = "数据存储配置", response = classOf[WriteWrapper]),
      new ApiResponse(code = 500, message = "Internal server error")
    ))
    @Path("")
    private def byId:Route = path(Segment){
      id =>
        parameter('pretty.?) {
          pretty =>
            onComplete[Option[WriteWrapper]]((server ? (Opt.GET, id)).asInstanceOf[Future[Option[WriteWrapper]]]) {
              case Success(Some(data)) =>
                complete(toJson(data, pretty))
              case Success(None) =>
                complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString, "113"), pretty))
              case Failure(ex) =>
                val time = Constants.CONF.getString("master.timeout")
                val hostname = Constants.CONF.getString("master.web.http.host")
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))
            }
        }
    }
    @ApiOperation(value = "检查数据存储合法性", notes = "检查数据存储合法性", httpMethod = "post")
    @ApiImplicitParams(Array(
      new ApiImplicitParam(name = "body", value = "消息体", required = true,
        dataTypeClass = classOf[WriteWrapper], paramType = "body")
    ))
    @ApiResponses(Array(
      new ApiResponse(code = 200, message = "数据存储列表", response = classOf[List[WriteWrapper]]),
      new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
    ))
    @Path("pre-check")
    private def preCheck:Route = path("pre-check"){
      entity(as[String]) {
        json =>
          val flag = try {
            mapper.readValue[WriteWrapper](json)
            true
          } catch {
            case e: Exception =>
              false
          }
          complete(s"""{"usable":$flag,"message":"上传文件不匹配"}""")

      }
    }

  @ApiOperation(value = "新增或者更新", notes = "新增或者更新数据存储", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[WriteWrapper], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[Result]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("")
  private def addOrUpdate:Route = entity(as[String]){
    json =>
      parameter('pretty.?) {
        pretty =>
          val Modeling = mapper.readValue[WriteWrapper](json)

          onComplete[BuildResult] {

            //   val dataSource = json.parseJson.convertTo[WriterWrapper]
            Modeling.id match {
              case null =>
                (server ? (Opt.CREATE, Modeling.copy(id = Option(UUID.randomUUID().toString)))).asInstanceOf[Future[BuildResult]]
              case _ =>
                (server ? (Opt.UPDATE, Modeling)).asInstanceOf[Future[BuildResult]]

            }
          } {
            case Success(value) =>
              complete(toJson(value, pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
            //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

          }
      }
  }

  @ApiOperation(value = "删除", notes = "根据ID删除数据存储", httpMethod = "delete")
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
            onComplete[BuildResult] {
              (server ? (Opt.DELETE, id)).asInstanceOf[Future[BuildResult]]
            } {
              case Success(value) =>
                complete(toJson(value, pretty))
              case Failure(ex) =>
                val time = Constants.CONF.getString("master.timeout")
                //timeout.toString
                val hostname = Constants.CONF.getString("master.web.http.host")
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))

            }
        }
    }
  }
  def route: Route = {
    pathPrefix("writer") {
      get {
        list ~ byId
      } ~
      // create a datasource
      post {
        preCheck ~ addOrUpdate
      } ~
        _delete

    }
  }
}

//trait WriterRouter extends DefaultRouter {
//
//  private lazy val server = context.actorOf(Props[WriteServer])
//  val writer = {
//    pathPrefix("writer") {
//      parameters('pretty.?) {
//        pretty =>
//          pathEndOrSingleSlash {
//            get {
//              parameters('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?) {
//                (start, limit, orderBy, order, search) =>
//                  val entry = search match {
//                    case Some(con: String) =>
//                      val data = mapper.readValue[Map[String, String]](con)
//                      if (data.isEmpty) {
//                        None
//                      } else {
//                        val name: String = data.get("name") match {
//                          case Some(_name: String) =>
//                            _name
//                          case _ =>
//                            null
//                        }
//
//                        Option(WriteWrapper(name = name, writeType = data.get("writeType"), data = null))
//                      }
//
//                    case _ =>
//                      None
//                  }
//
//                  start match {
//                    case Some(_) =>
//                      onComplete[Pagination[WriteWrapper]] {
//                        (server ? (start.getOrElse("0").toInt, limit.getOrElse("10").toInt, orderBy, order, entry)).asInstanceOf[Future[Pagination[WriteWrapper]]]
//                      } {
//                        case Success(value) => complete(toJson(value, pretty))
//                        case Failure(ex) =>
//                          complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                        //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                      }
//                    case None =>
//                      onComplete[List[WriteWrapper]]((server ? Opt.GET).asInstanceOf[Future[List[WriteWrapper]]]) {
//                        case Success(value) => complete(toJson(value, pretty))
//                        case Failure(ex) =>
//                          complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                        //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                      }
//
//                  }
//
//              }
//            } ~
//              post {
//                requestEncodedWith(HttpEncoding.custom("UTF-8")) {
//                  entity(as[String]) {
//                    json =>
//                      val _writer: WriteWrapper = mapper.readValue[WriteWrapper](json)
//                      val writer = _writer.id match {
//                        case Some(id) =>
//                          _writer
//                        case None =>
//                          _writer.copy(id = Some(UUID.randomUUID().toString))
//                      }
//                      onComplete[BuildResult] {
//                        (server ? (Opt.UPDATE, writer)).asInstanceOf[Future[BuildResult]]
//                      } {
//                        case Success(value) =>
//                          complete(toJson(value,pretty))
//                        case Failure(ex)=>
//                          complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                        //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                      }
//                  }
//                }
//              }
//          } ~path("pre-check"){
//            post {
//              entity(as[String]) {
//                json =>
//                  var msg = ""
//                  val flag=  try {
//                    val config = mapper.readValue[WriteWrapper](json)
//                    true
//                  }catch {
//                    case e:Exception=>
//                      msg = e.getMessage
//                      false
//                  }
//                  complete(s"""{"usable":${flag},"message":"上传文件不匹配${msg}"}""")
//
//              }
//            }
//          }~
//            pathPrefix(Segment) { writerId =>
//
//              get {
//                onComplete[Option[WriteWrapper]] {
//                  (server ? (Opt.GET, writerId)).asInstanceOf[Future[Option[WriteWrapper]]]
//                } {
//                  case Success(value) =>
//                    value match {
//                      case Some(info) =>
//                        complete(toJson(info, pretty))
//                      case _ =>
//                        complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString,"613"), pretty))
//                      //complete(toJson(Result(StatusCodes.NotFound.intValue.toString, s"Writer[$writerId] NOT FOUND"), pretty))
//                    }
//                  case Failure(ex) =>
//                    complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage), pretty))
//                  //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                }
//              } ~
//                delete {
//                  onComplete[BuildResult] {
//                    (server ? (Opt.DELETE, writerId)).asInstanceOf[Future[BuildResult]]
//                  } {
//                    case Success(value) =>
//                      complete(toJson(value, pretty))
//                    case Failure(ex) =>
//                      complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage),pretty))
//                    //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
//
//                  }
//                }
//
//            }
//      }
//    }
//  }
//
//}