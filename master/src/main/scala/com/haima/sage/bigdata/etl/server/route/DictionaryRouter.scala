package com.haima.sage.bigdata.etl.server.route

import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.HttpEncoding
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store.Stores
import io.swagger.annotations._

/**
  * Created: 2016-05-25 09:31.
  * Author:zhhuiyan
  * Created: 2016-05-25 09:31.
  *
  *
  */
@Api(value = "目录",produces = "application/json")
@Path("dictionary")
class DictionaryRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {
  @ApiOperation(value = "获得目录", notes = "当start为空时，返回目录信息list[]", httpMethod = "get")
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
    new ApiResponse(code = 200, message = "目录", response = classOf[String]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def dictionary:Route = pathEndOrSingleSlash {
    parameters('pretty.?) {
      pretty =>
        parameters('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?) {
          (startO, limitO, orderBy, order, search) =>
            complete {
              val start = startO match {
                case Some(s) => s.toInt
                case None => 0
              }
              val limit = limitO match {
                case Some(l) => l.toInt
                case None => 20
              }
              val rs = Stores.dictionaryStore.queryByPage(start, limit, orderBy, order, search.map(d => mapper.readValue[Dictionary](d)))

              toJson(rs, pretty)

            }
        }
    }
  }

  @ApiOperation(value = "获取目录",notes="获取目录",httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[Dictionary], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则配置", response = classOf[Dictionary]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
    private def showDictionary:Route = pathEndOrSingleSlash {
    parameter('pretty.?) {
      pretty =>
        requestEncodedWith(HttpEncoding.custom("UTF-8")) {
          entity(as[String]) { json =>
            complete {
              logger.info("receive [create a dictionary] json:{}", json)
              val dictionary = mapper.readValue[Dictionary](json)
              val datas: Map[String, (String, String)] = dictionary.properties.map {
                case prop =>
                  (prop.key, (prop.value, prop.vtype))
              }.toMap
              val rt = if (Stores.dictionaryStore.set(dictionary)) {

                /*
               *TODO
              Master.workers.foreach {
                case (id, worker) =>
                  worker ! ("dictionary", dictionary.key, datas)
              }*/

                BuildResult(StatusCodes.OK.intValue.toString, "906", dictionary.name)
                //Result(StatusCodes.OK.intValue.toString, "PUT DICTIONARY SUCCESS")
              }

              else {
                BuildResult(StatusCodes.InternalServerError.intValue.toString, "907", dictionary.name)
                //Result(StatusCodes.InternalServerError.intValue.toString, "PUT DICTIONARY FAILED")
              }

              toJson(rt, pretty)
            }

          }
        }

    }
  }
  @ApiOperation(value = "获取目录",notes="根据ID获取目录",httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "dictionaryId", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则配置", response = classOf[AnalyzerWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
  private def byId:Route = pathPrefix(Segment) {
    dictionaryId =>
      parameter('pretty.?) {
        pretty =>
          complete {
            logger.info(s"query dictionary id[$dictionaryId]")
            val rs = Stores.dictionaryStore.get(dictionaryId) match {
              case Some(info) => info
              case None =>
                BuildResult(StatusCodes.NotFound.intValue.toString, "910")
              //Result(StatusCodes.NotFound.intValue.toString, s"dictionary $dictionaryId NOT FOUND")
            }
            toJson(rs, pretty)
          }
      }
  }

  @ApiOperation(value = "删除", notes = "根据ID删除目录", httpMethod = "delete")
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
    parameter('pretty.?){
      pretty=>
      path(Segment){
        dictionaryId=>
        complete {
          val rs = if (Stores.dictionaryStore.delete(dictionaryId))
            BuildResult(StatusCodes.OK.intValue.toString,"908")
          //Result(StatusCodes.OK.intValue.toString, s"DELETE dictionary $dictionaryId SUCCESS")
          else
            BuildResult(StatusCodes.InternalServerError.intValue.toString,"909")
          //Result(StatusCodes.InternalServerError.intValue.toString, "DELETE dictionary ID:" + dictionaryId + "FAILED")
          toJson(rs, pretty)
        }

      }

    }
  }

  def route:Route = {
    pathPrefix("dictionary"){
      get{
        dictionary ~ byId
      } ~
      post{
        showDictionary
      } ~
      _delete
    }
  }
}