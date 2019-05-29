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
import com.haima.sage.bigdata.etl.server.hander.ConfigServer
import io.swagger.annotations._

import scala.concurrent._
import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2015/6/18.
  */
@Api(value = "数据通道配置", produces = "application/json")
@Path("/config/")
class ConfigRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {

  // override implicit lazy val timeout: Timeout = Timeout(5000.toLong, java.util.concurrent.TimeUnit.MILLISECONDS)
  private lazy val server = context.actorOf(Props[ConfigServer])

  // 配置信息的设置、更新
  @ApiOperation(value = "获取分页的数据通道配置信息", notes = "获取分页的数据通道配置信息,当`start` 是空时,返回所有数据:list[]", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "start", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "limit", value = "条数", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "orderBy", value = "排序字段", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "order", value = "正序,反序", required = false,
      dataTypeClass = classOf[String], paramType = "form"),
    new ApiImplicitParam(name = "search", value = "检索条件", required = false,
      dataTypeClass = classOf[ConfigWrapper], paramType = "form")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "分页的数据列表", response = classOf[Pagination[ConfigWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list: Route = pathEndOrSingleSlash {
    parameter('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?, 'pretty.?) {
      (_start, limit, orderBy, order, search, pretty) =>

        _start match {
          case Some(start) if start.matches("\\d+") =>

            val entry = search match {
              case Some(con: String) =>
                val data = mapper.readValue[Map[String, String]](con)
                Option(ConfigWrapper(name = data.getOrElse("name", null)))
              case _ =>
                None
            }
            onComplete[Pagination[ConfigWrapper]] {
              (server ? (start.toInt, limit.getOrElse("10").toInt, orderBy, order, entry)).asInstanceOf[Future[Pagination[ConfigWrapper]]]
            } {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                val time = Constants.CONF.getString("master.timeout")
                val hostname = Constants.CONF.getString("master.web.http.host")
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))

            }
          case None =>
            onComplete[List[ConfigWrapper]]((server ? Opt.GET).asInstanceOf[Future[List[ConfigWrapper]]]) {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                val time = Constants.CONF.getString("master.timeout")
                val hostname = Constants.CONF.getString("master.web.http.host")
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))

            }

        }
    }
  }

  @Path("{id}")
  @ApiOperation(value = "获取数据通道配置", notes = "根据ID获取数据通道配置", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "通道配置", response = classOf[ConfigWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def byId: Route = path(Segment) {
    id =>
      parameter('pretty.?) {
        pretty =>
          onComplete[Option[ConfigWrapper]]((server ? (Opt.GET, id)).asInstanceOf[Future[Option[ConfigWrapper]]] /*.map(_.map(toConfig))*/) {
            case Success(Some(data)) =>
              complete(toJson(data, pretty))
            case Success(None) =>
              complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString, "711"), pretty))
            //complete(toJson(Result(StatusCodes.NotFound.intValue.toString, s"CONFIG[$id] NOT FOUND"), pretty))
            case Failure(ex) =>
              val time = Constants.CONF.getString("master.timeout")
              val hostname = Constants.CONF.getString("master.web.http.host")
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))
          }
      }
    // get all config info
  }

  /*// 配置信息的设置、更新
  @ApiOperation(value = "获取分页的配置信息", notes = "获取分页的配置信息,当`start` 是空时,返回所有不分页的数据", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "数据列表", response = classOf[List[ConfigWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def all: Route = pathEndOrSingleSlash {
    parameter('pretty.?) {
      pretty =>
        onComplete[List[ConfigWrapper]]((server ? Opt.GET).asInstanceOf[Future[List[ConfigWrapper]]] /*.map(_.map(toConfig))*/) {
          case Success(data) =>
            complete(toJson(data, pretty))
          case Failure(ex) =>
            val time = Constants.CONF.getString("master.timeout")
            val hostname = Constants.CONF.getString("master.web.http.host")
            complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))
        }
    }
  }*/

  // 配置信息的设置、更新
  @ApiOperation(value = "检查配置的合法性", notes = "检查配置的合法性", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[ConfigWrapper], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "数据列表", response = classOf[List[ConfigWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("pre-check")
  private def preCheck: Route = path("pre-check") {

    entity(as[String]) {
      json =>
        val flag = try {
          mapper.readValue[ConfigWrapper](json)
          true
        } catch {
          case e: Exception =>
            false
        }
        complete(s"""{"usable":$flag,"message":"上传文件不匹配"}""")

    }

  }

  @ApiOperation(value = "新增或者更新", notes = "新增或者更新通道配置", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[ConfigWrapper], paramType = "body")

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
          val config = mapper.readValue[ConfigWrapper](json)
          onComplete[BuildResult] {

            Option(config.id) match {
              case Some(_) =>
                (server ? (Opt.UPDATE, config)).asInstanceOf[Future[BuildResult]]
              case None =>
                (server ? (Opt.CREATE, config.copy(id = UUID.randomUUID().toString))).asInstanceOf[Future[BuildResult]]
            }
          } {
            case Success(value) => complete(toJson(value, pretty))
            case Failure(ex) =>
              val time = Constants.CONF.getString("master.timeout")
              val hostname = Constants.CONF.getString("master.web.http.host")
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))

          }
      }
  }


  @ApiOperation(value = "启动/停止", notes = "启动/停止数据通道", httpMethod = "put")
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

  @ApiOperation(value = "更新", notes = "更新通道配置", httpMethod = "put")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = false,
      dataTypeClass = classOf[ConfigWrapper], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[Result]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  private def update: Route = pathEndOrSingleSlash {
    entity(as[String]) {
      json =>
        parameter('pretty.?) {
          pretty =>
            val config = mapper.readValue[ConfigWrapper](json)
            onComplete[BuildResult] {
              (server ? (Opt.UPDATE, config)).asInstanceOf[Future[BuildResult]]
            } {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                val time = Constants.CONF.getString("master.timeout")
                val hostname = Constants.CONF.getString("master.web.http.host")
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", hostname, time), pretty))

            }
        }
    }
  }


  @ApiOperation(value = "删除", notes = "根据ID删除数据通道", httpMethod = "delete")
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
    pathPrefix("config") {
      get {
        list ~ byId
      } ~
        // create a datasource
        post {
          preCheck ~ addOrUpdate
        } ~
        put {
          startOrStop ~ update
        } ~ _delete

    }
  }
}
