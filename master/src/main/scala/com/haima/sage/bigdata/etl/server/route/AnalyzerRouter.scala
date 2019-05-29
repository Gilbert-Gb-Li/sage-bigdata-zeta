package com.haima.sage.bigdata.etl.server.route

import java.util.UUID
import javax.ws.rs.Path

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.HttpEncoding
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.AnalyzerServer
import io.swagger.annotations._

import scala.concurrent.Future
import scala.util.{Failure, Success}


/**
  * @version 2016-05-13 14:00.
  * @author zhhuiyan
  *
  *
  */
@Api(value = "分析规则使用", produces = "application/json")
@Path("/analyzer/")
class AnalyzerRouter(override val context:akka.actor.ActorSystem) extends DefaultRouter{
  private lazy val server = context.actorOf(Props[AnalyzerServer])

  @ApiOperation(value = "获分析规则信息", notes = "当start为空时，返回所分析规则信息list[]", httpMethod = "get")
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
    new ApiResponse(code = 200, message = "分页的数据列表", response = classOf[Pagination[AnalyzerWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list:Route = pathEndOrSingleSlash{
    parameters('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?,'pretty.?) {
      (start, limit, orderBy, order, search,pretty) =>
        start match {
          case Some(data) if data.matches("\\d+") =>
            onComplete[Pagination[AnalyzerWrapper]] {
              (server ? (data.toInt, limit.getOrElse("10").toInt, orderBy, order, search.map(mapper.readValue[AnalyzerWrapper]))).asInstanceOf[Future[Pagination[AnalyzerWrapper]]]
            } {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
            }
          case None =>
            onComplete[List[AnalyzerWrapper]]((server ? Opt.GET).asInstanceOf[Future[List[AnalyzerWrapper]]]) {
              case Success(value) => complete(toJson(value, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"901",ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }

        }
    }
  }

  @ApiOperation(value = "获取分析规则",notes="根据ID获取分析规则",httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "想获取的分析规则ID", required = false,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则配置", response = classOf[AnalyzerWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("{id}")
  private def byId:Route = path(Segment){
    id =>
      parameter('pretty.?) {
        pretty =>
          onComplete[Option[AnalyzerWrapper]]((server ? (Opt.GET, id)).asInstanceOf[Future[Option[AnalyzerWrapper]]]) {
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
      dataTypeClass = classOf[AnalyzerWrapper], paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则列表", response = classOf[List[AnalyzerWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("pre-check")
  private def preCheck:Route = path("pre-check"){
    entity(as[String]) {
      json =>
        val flag = try {
          mapper.readValue[AnalyzerWrapper](json)
          true
        } catch {
          case e: Exception =>
            false
        }
        complete(s"""{"usable":$flag,"message":"上传文件不匹配"}""")

    }
  }

  @ApiOperation(value = "分析规则预览", notes = "分析规则预览", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[AnalyzerWrapper], paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "规则列表", response = classOf[List[AnalyzerWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("preview")
  private def analyzerPreview:Route = path("preview"){
    requestEncodedWith(HttpEncoding.custom("UTF-8")) {
      parameter('pretty.?){
        pretty=>
          entity(as[String]) { json =>
            logger.info("receive [preview a AnalyzerWrapper] json:{}", json)
            DataSource.previewersInfo.get("flink") match {
              case Some(collector) =>
                val analyzerWrapper: AnalyzerWrapper = mapper.readValue[AnalyzerWrapper](json)
                onComplete[(List[Map[String,Any]],_)] {
                  (server .? (Opt.PREVIEW,collector,analyzerWrapper)(previewTimeout)).asInstanceOf[Future[(List[Map[String,Any]],_)]]
                } {
                  case Success(t) =>
                    val value=t._1
                    if(value.nonEmpty){
                      val cNames = value.head.keys
                      if(cNames.mkString(",")=="error"){
                        complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901",value.head("error").toString()), pretty))
                      }else{
                        complete(toJson(Map("cNames"->cNames,"data"-> value), pretty))
                      }
                    }else{
                      complete(toJson(Map(), pretty))
                    }
                  case Failure(ex) =>
                    complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901",ex.getMessage), pretty))
                }
              case None =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "1301"), pretty))
            }
          }
      }

    }
  }

  @ApiOperation(value = "新增或者更新", notes = "新增或者更新分析规则", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[AnalyzerWrapper], paramType = "body")

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
            val analyzer = mapper.readValue[AnalyzerWrapper](json)

            onComplete[BuildResult] {

              //   val dataSource = json.parseJson.convertTo[AnalyzerWrapper]
              analyzer.id match {
                case null =>
                  (server ? (Opt.CREATE, analyzer.copy(id = Option(UUID.randomUUID().toString)))).asInstanceOf[Future[BuildResult]]
                case _ =>
                  (server ? (Opt.UPDATE, analyzer)).asInstanceOf[Future[BuildResult]]

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


  @ApiOperation(value = "删除", notes = "根据ID删分析规则", httpMethod = "delete")
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
    pathPrefix("analyzer") {
      get {
        list ~ byId
      } ~
        post {
          preCheck ~ analyzerPreview ~ addOrUpdate
        } ~
        _delete

    }
  }

}
