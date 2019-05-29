package com.haima.sage.bigdata.etl.server.route



import javax.ws.rs.Path

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.KnowledgeInfoServer
import io.swagger.annotations._

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by liyju on 2017/11/20.
  */
@Api(value = "知识库管理",produces = "application/json")
@Path("/knowledgeinfo/")
class KnowledgeInfoRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {
  private lazy val serverInfo = context.actorOf(Props[KnowledgeInfoServer])

  @ApiOperation(value = "获取分页的知识库相关信息", notes = "当start为空时，返回所有知识库信息list[]", httpMethod = "get")
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
    new ApiResponse(code = 200, message = "分页的数据建模列表", response = classOf[Pagination[mutable.Map[String, String]]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def knowledgeInfo: Route = pathEndOrSingleSlash {
      get {
        parameters('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?,'Pretty.?) {
          (start, limit, orderBy, order, search,pretty) =>
            val entry = search match {
              case Some(con: String) =>
                val data = mapper.readValue[Map[String, String]](con)
                if (data.isEmpty) {
                  None
                } else {
                  Option(data.getOrElse("tableName", null))
                }
              case _ =>
                None
            }
            onComplete[Pagination[mutable.Map[String, String]]] {
              (serverInfo ? (start.getOrElse("0").toInt, limit.getOrElse("10").toInt, orderBy, order, entry)).asInstanceOf[Future[Pagination[mutable.Map[String, String]]]]
            } {
              case Success(value) =>
                complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
            }
        }
      }
    }

  def route:Route = {
    pathPrefix("knowledgeInfo"){
      get{
        knowledgeInfo
      }
    }
  }
}


