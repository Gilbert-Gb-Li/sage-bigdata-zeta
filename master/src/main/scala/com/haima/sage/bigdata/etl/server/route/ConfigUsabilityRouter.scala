package com.haima.sage.bigdata.etl.server.route

import javax.ws.rs.Path

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.ConfigServer
import io.swagger.annotations._

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2015/6/18.
  */
@Api(value = "数据通道检查",produces = "application/json")
@Path("/usability/")
class ConfigUsabilityRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {
  private lazy val server = context.actorOf(Props[ConfigServer])
  @ApiOperation(value = "数据通道检查",notes = "数据通道是否可用",httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则配置", response = classOf[AnalyzerWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
  private def usability:Route = parameter('pretty.?) {
    pretty =>
      path(Segment) {
        id =>
          onComplete[List[Usability]] {
            logger.info(s" check usable for config[$id]")
            (server ? ("usable", id)).asInstanceOf[Future[List[Usability]]]
          } {
            case Success(value) => complete(toJson(value, pretty))
            case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
          }
      }
  }

  @ApiOperation(value = "数据通道检查",notes = "数据通道是否可用",httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则配置", response = classOf[AnalyzerWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
  private def checkConfig:Route = pathEndOrSingleSlash {
    parameter('pretty.?){
      pretty=>
        entity(as[String]) {
          data => {
            onComplete[List[Usability]] {
              val config = mapper.readValue[ConfigWrapper](data)
              (server ? ("usable", config.id)).asInstanceOf[Future[List[Usability]]]
            } {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")

            }
          }
        }
    }
  }

  def route:Route = {
    pathPrefix("usability"){
      get{
        usability
      } ~
      post{
        checkConfig
      }
    }
  }
}

