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
import com.haima.sage.bigdata.etl.server.hander.{ParserServer}
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
@Api(value = "解析规则", produces = "application/json")
@Path("/Parser/")
class ParserRouter(override val context:akka.actor.ActorSystem) extends DefaultRouter {
  private lazy val server = context.actorOf(Props[ParserServer])

  @ApiOperation(value = "获取解析规则信息", notes = "当start为空时，返回所有解析规则信息list[]", httpMethod = "get")
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
    new ApiResponse(code = 200, message = "分页的解析规则列表", response = classOf[Pagination[ParserWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list: Route = pathEndOrSingleSlash {
    parameter('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?, 'pretty.?) {
      (start: Option[String], limit: Option[String], orderBy: Option[String],
       order: Option[String], search: Option[String], pretty: Option[String]) =>
        val entry = search match {
          case Some(con: String) =>
            val data = mapper.readValue[Map[String, String]](con)
            //  val data=con.parseJson.convertTo[Map[String,String]]
            Option(ParserWrapper(name = data.getOrElse("name", null)))
          case _ =>
            None
        }
        start match {
          case Some(data) =>
            onComplete[Pagination[ParserWrapper]] {
              (server ? (data.toInt, limit.getOrElse("10").toInt, orderBy, order, entry)).asInstanceOf[Future[Pagination[ParserWrapper]]]
            } {
              case Success(value) =>
                complete(toJson(value, pretty))
              case Failure(ex) =>
                ex.printStackTrace()
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }
          case _ =>
            onComplete[List[ParserWrapper]] {
              (server ? Opt.GET).asInstanceOf[Future[List[ParserWrapper]]]
            } {
              case Success(value) =>
                complete(toJson(value, pretty))
              case Failure(ex) =>
                ex.printStackTrace()
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }
        }
    }
  }

  @ApiOperation(value = "获取解析规则", notes = "根据ID获取解析规则", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "想获取的解析规则ID", required = true,
      dataTypeClass = classOf[String], paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则配置", response = classOf[Parser[_]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("{id}")
  private def byId: Route = path(Segment) {
    id =>
      parameter('pretty.?) {
        pretty =>
          onComplete[Option[ParserWrapper]]((server ? (Opt.GET, id)).asInstanceOf[Future[Option[ParserWrapper]]]) {
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

  @ApiOperation(value = "检查规则合法性", notes = "检查规则合法性", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[Parser[_]], paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则列表", response = classOf[List[Parser[_]]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("pre-check")
  private def preCheck: Route = path("pre-check") {
    entity(as[String]) {
      json =>
        val flag = try {
          mapper.readValue[ParserWrapper](json)
          true
        } catch {
          case e: Exception =>
            false
        }
        complete(s"""{"usable":$flag,"message":"上传文件不匹配"}""")

    }
  }

  @ApiOperation(value = "新增或者更新", notes = "新增或者更新分析规则", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[Parser[_]], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[Result]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("")
  private def addOrUpdate: Route = entity(as[String]) {
    json =>
      parameter('pretty.?) {
        pretty =>
          val Parser = mapper.readValue[ParserWrapper](json)

          onComplete[BuildResult] {

            //   val dataSource = json.parseJson.convertTo[ParserWrapper]
            Parser.id match {
              case null =>
                (server ? (Opt.CREATE, Parser.copy(id = Option(UUID.randomUUID().toString)))).asInstanceOf[Future[BuildResult]]
              case _ =>
                (server ? (Opt.UPDATE, Parser)).asInstanceOf[Future[BuildResult]]

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

  @ApiOperation(value = "删除", notes = "根据ID删除解析规则", httpMethod = "delete")
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
  private def _delete = delete {
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
    pathPrefix("parser") {
      get {
        list ~ byId
      } ~
      post {
        addOrUpdate ~ preCheck
      } ~
        _delete
    }
  }

}
