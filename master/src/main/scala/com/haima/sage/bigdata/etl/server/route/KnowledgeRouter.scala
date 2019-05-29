package com.haima.sage.bigdata.etl.server.route

import java.util.UUID
import javax.ws.rs.Path

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{pathPrefix, _}
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.knowledge.Knowledge
import com.haima.sage.bigdata.etl.server.hander.KnowledgeServer
import io.swagger.annotations._

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * @version 2016-07-26 18:00.
  * @author sunran
  *
  */
@Api(value = "知识库配置", produces = "application/json")
@Path("/knowledge/")
class KnowledgeRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {
  private lazy val server = context.actorOf(Props[KnowledgeServer])

  @ApiOperation(value = "获取分页的知识库配置信息", notes = "获取分页的知识库配置信息,当`start` 是空时,返回所有数据:list[]", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "start", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "form", defaultValue = "0"),
    new ApiImplicitParam(name = "limit", value = "条数", required = false,
      dataTypeClass = classOf[String], paramType = "form", defaultValue = "10"),
    new ApiImplicitParam(name = "orderBy", value = "排序字段", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "order", value = "正序,反序", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "search", value = "检索条件", required = false,
      dataTypeClass = classOf[Knowledge], paramType = "form")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "分页的数据列表", response = classOf[Pagination[Knowledge]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list: Route = pathEndOrSingleSlash {
    get {
      parameters('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?, 'pretty.?) {
        (start: Option[String], limit: Option[String], orderBy: Option[String], order: Option[String], search: Option[String], pretty: Option[String]) =>
          val entry = search match {
            case Some(con: String) =>
              val data = mapper.readValue[Map[String, String]](con)
              if (data.isEmpty) {
                None
              } else {
                Option(Knowledge(name = Option(data.getOrElse("name", null))))
              }
            case _ =>
              None
          }
          start match {
            case Some(_) =>
              onComplete[Pagination[Knowledge]] {
                (server ? (start.getOrElse("0").toInt, limit.getOrElse("10").toInt, orderBy, order, entry)).asInstanceOf[Future[Pagination[Knowledge]]]
              } {
                case Success(value) =>
                  complete(toJson(value, pretty))
                case Failure(ex) =>
                  complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), pretty))
              }
            case None =>
              onComplete[List[Knowledge]]((server ? Opt.GET).asInstanceOf[Future[List[Knowledge]]]) {
                case Success(value) => complete(toJson(value, pretty))
                case Failure(ex) =>
                  complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), pretty))

              }
          }
      }
    }
  }

  @ApiOperation(value = "新增或者更新", notes = "新增或者更新知识库配置", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[Knowledge], paramType = "body"),
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query")

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
          val _knowledge = mapper.readValue[Knowledge](json)
          logger.info(s"add knowledge[${_knowledge}]")
          val knowledge = _knowledge.id match {
            case _: String => _knowledge
            case _ =>
              _knowledge.copy(id = UUID.randomUUID().toString)
          }
          onComplete[BuildResult] {
            (server ? (Opt.UPDATE, knowledge)).asInstanceOf[Future[BuildResult]]
          } {
            case Success(value) =>
              complete(toJson(value, pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), pretty))
          }
      }
  }

  // 配置信息的设置、更新
  @ApiOperation(value = "检查知识库配置的合法性", notes = "检查知识库配置的合法性", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[Knowledge], paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "数据列表", response = classOf[List[Knowledge]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("pre-check")
  private def preCheck: Route = path("pre-check") {
    entity(as[String]) {
      json =>
        var msg = ""
        val flag = try {
          mapper.readValue[Knowledge](json)
          true
        } catch {
          case e: Exception =>
            msg = e.getMessage
            false
        }
        complete(s"""{"usable":$flag,"message":"上传文件不匹配$msg"}""")

    }
  }


  @Path("{id}")
  @ApiOperation(value = "获取知识库配置", notes = "根据ID获取知识库配置", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "知识库ID", required = false,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "知识库", response = classOf[Knowledge]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def byId: Route = path(Segment) {
    knowledgeId =>
      parameter('pretty.?) {
        pretty =>
          onComplete[Option[Knowledge]] {
            (server ? (Opt.GET, knowledgeId)).asInstanceOf[Future[Option[Knowledge]]]
          } {
            case Success(value) =>
              value match {
                case Some(info) =>
                  complete(toJson(info, pretty))
                case _ =>
                  complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString, "512"), pretty))
              }
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), pretty))
          }
      }
  }

  @ApiOperation(value = "删除", notes = "根据ID删除知识库", httpMethod = "delete")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "知识库ID", required = true,
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
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), pretty))
            }
        }
    }
  }

  @ApiOperation(value = "启动/停止", notes = "启动/停止知识库", httpMethod = "put")
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
    (opt: String, id) => {
      parameter('pretty.?) {
        pretty =>
          onComplete[BuildResult] {
            (server ? (Opt.withName(opt.toUpperCase()), id)).asInstanceOf[Future[BuildResult]]
          } {
            case Success(value) =>
              complete(toJson(value, pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), pretty))
          }
      }
    }
  }

  @ApiOperation(value = "获取状态是完成的知识库列表", notes = "获取状态是完成的知识库列表", httpMethod = "post")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[List[Knowledge]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("queryByStatus")
  private def queryByStatus: Route = path("queryByStatus") {
      onComplete[List[Knowledge]] {
        (server ? "queryByStatus").asInstanceOf[Future[List[Knowledge]]]
      } {
        case Success(value) =>
          complete(toJson(value, None))
        case Failure(ex) =>
          complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), None))
      }
  }

  def route: Route = {
    pathPrefix("knowledge") {
      get {
        list ~ byId
      } ~
        post {
          preCheck ~ queryByStatus~ addOrUpdate
        } ~
        put {
          startOrStop
        } ~ _delete
    }
  }
}