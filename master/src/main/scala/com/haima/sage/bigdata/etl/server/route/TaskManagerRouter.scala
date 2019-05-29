package com.haima.sage.bigdata.etl.server.route

import javax.ws.rs.Path

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{get, _}
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.TaskManagerServer
import io.swagger.annotations._

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2017/3/23.
  *
  */

@Api(value = "任务管理配置", produces = "application/json")
@Path("/taskManager/")
class TaskManagerRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {

  private lazy val server = context.actorOf(Props[TaskManagerServer])
  // 任务调度信息列表
  @ApiOperation(value = "获取分页任务信息列表", notes = "获取分页的调度任务信息,当`start` 是空时,返回所有数据:list[]", httpMethod = "get")
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
      dataTypeClass = classOf[TaskWrapper], paramType = "form")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "分页的数据列表", response = classOf[Pagination[TaskWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list: Route = pathEndOrSingleSlash {
    parameter('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?, 'pretty.?) {
      (start, limit, orderBy, order, search,pretty) =>
        val entry = search match {
          case Some(con: String) =>
            val data = mapper.readValue[Map[String, String]](con)
            val task = if(data.getOrElse("jobType",null)==null) null else Task(configId=null,jobType =data.get("jobType") )
            Option(TaskWrapper( name=data.getOrElse("name", null),data=task))
          case _ =>
            None
        }
        start match {
          case Some(_start) =>
            onComplete[Pagination[TaskWrapper]] {

              (server ? (_start.toInt,  limit.getOrElse("10").toInt, orderBy, order,entry)).asInstanceOf[Future[Pagination[TaskWrapper]]]

            } {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"926",ex.getMessage), pretty))

            }
          case None =>

            onComplete[List[TaskWrapper]]((server ? Opt.GET).asInstanceOf[Future[List[TaskWrapper]]]) {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"926",ex.getMessage), pretty))

            }
        }
    }
  }

  @Path("{id}")
  @ApiOperation(value = "获取调度任务信息", notes = "根据ID获取调度任务信息", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "唯一标识ID", required = true,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "调度任务信息", response = classOf[TaskWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def byId: Route = path(Segment) {
    id =>
      parameter('pretty.?) {
        pretty =>
          onComplete[Option[TaskWrapper]]((server ? (Opt.GET, id)).asInstanceOf[Future[Option[TaskWrapper]]]) {
            case Success(value) =>
              complete(toJson(value, pretty))
            case Failure(ex)
            => complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"926",ex.getMessage), pretty))

          }
      }
  }

  //获取任务调度msg信息
  @ApiOperation(value = "获取调度任务日志列表", notes = "根据ID获取调度任务日志信息", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "taskId", value = "调度任务ID", required = true,
      dataTypeClass = classOf[String], paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[List[TaskLogInfoWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("timerId/{timerId}")
  private def  getTimerMsg : Route=path("taskId"/Segment) {
    taskId =>
      parameter('pretty.?) {
        pretty =>
          onComplete[List[TaskLogInfoWrapper]] {
            (server ? (Opt.GET,"msg",taskId)).asInstanceOf[Future[List[TaskLogInfoWrapper]]]
          } {
            case Success(value) =>
              complete(toJson(value, pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), pretty))

          }
      }
    }

  // 上传校验
  @ApiOperation(value = "检查配置的合法性", notes = "检查配置的合法性", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[TaskWrapper], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "数据列表", response = classOf[List[TaskWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[BuildResult])
  ))
  @Path("pre-check")
  private def preCheck: Route = path("pre-check") {

    entity(as[String]) {
         json =>
          var msg = ""
          val flag=  try {
            mapper.readValue[TaskWrapper](json)
            true
          }catch {
            case e:Exception=>
              msg = e.getMessage
              false
          }
          complete(s"""{"usable":$flag,"message":"上传文件不匹配$msg"}""")

    }

  }

  @ApiOperation(value = "新增或者更新调度任务信息", notes = "新增或者更新调度任务信息", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[TaskWrapper], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[BuildResult]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[BuildResult])
  ))
  @Path("")
  private def addOrUpdate(): Route = entity(as[String]) {
    json =>
      parameter('pretty.?) {
        pretty =>
          val wrapper = mapper.readValue[TaskWrapper](json)
          onComplete[BuildResult] {
            (server ? (Opt.UPDATE, wrapper)).asInstanceOf[Future[BuildResult]]
          } {
            case Success(value) =>
              complete(toJson(value, pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "926", ex.getMessage), pretty))
          }
      }
  }

  @ApiOperation(value = "删除", notes = "根据ID删除调度任务信息", httpMethod = "delete")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "唯一标识ID", required = true,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[BuildResult]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("{id}")
  private def _delete = delete {
    path(Segment) {
      id =>
        parameter('pretty.?) {
          pretty =>
            onComplete[BuildResult] {
              (server ? (Opt.SYNC, (Opt.DELETE, id))).asInstanceOf[Future[BuildResult]]
            } {
              case Success(value) if StatusCodes.OK.intValue.toString.equals(value.status) =>
                complete(toJson(BuildResult(StatusCodes.OK.intValue.toString,"1004"), pretty))
              case Success(_) =>
                complete(toJson(BuildResult(StatusCodes.NotAcceptable.intValue.toString,"1006"), pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"926",ex.getMessage), pretty))
            }
        }
    }
  }

  def route: Route = {
    pathPrefix("taskManager") {
      get {
        list ~ byId ~getTimerMsg
      }~
        post {
          preCheck ~ addOrUpdate()
        }~_delete
    }
  }
}

