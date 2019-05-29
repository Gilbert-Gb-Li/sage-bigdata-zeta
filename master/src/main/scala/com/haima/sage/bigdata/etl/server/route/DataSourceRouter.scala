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
import com.haima.sage.bigdata.etl.server.hander.{DataSourceServer}
import io.swagger.annotations._

import scala.concurrent._
import scala.util.{Failure, Success}

/**
  * Created by ad on 2018/3/26.
  */
@Api(value = "数据源配置", produces = "application/json")
@Path("/datasource/")
class DataSourceRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {
  private lazy val server = context.actorOf(Props[DataSourceServer])
  @ApiOperation(value = "获取分页的数据源信息", notes = "当start为空时，返回所有数据源信息list[]", httpMethod = "get")
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
    new ApiResponse(code = 200, message = "分页的数据列表", response = classOf[Pagination[DataSourceWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def list: Route = 
    pathEndOrSingleSlash {
      parameter('start.?, 'limit.?, 'orderBy.?, 'order.?, 'search.?, 'pretty.?) {
        (start: Option[String], limit: Option[String], orderBy: Option[String],
         order: Option[String], search: Option[String], pretty: Option[String]) =>
          val entry = search match {
            case Some(con: String) =>
              val data = mapper.readValue[Map[String, String]](con)
              //  val data=con.parseJson.convertTo[Map[String,String]]
              Option(DataSourceWrapper(name = data.getOrElse("name", null)))
            case _ =>
              None
          }
          start match {
            case Some(data) =>
              onComplete[Pagination[DataSourceWrapper]] {
                (server ? (data.toInt, limit.getOrElse("10").toInt, orderBy, order, entry)).asInstanceOf[Future[Pagination[DataSourceWrapper]]]
              } {
                case Success(value) =>
                  complete(toJson(value, pretty))
                case Failure(ex) =>
                  ex.printStackTrace()
                  complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
                //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

              }
            case _ =>
              onComplete[List[DataSourceWrapper]] {
                (server ? Opt.GET).asInstanceOf[Future[List[DataSourceWrapper]]]
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

  @ApiOperation(value = "新增或者更新", notes = "新增或者更新数据源", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[DataSourceWrapper], paramType = "body")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "操作结果", response = classOf[Result]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("")
  private def addOrUpdate: Route = pathEndOrSingleSlash{
    entity(as[String]) {
      json =>
        parameter('pretty.?) {
          pretty =>
            val dataSource = mapper.readValue[DataSourceWrapper](json)

            onComplete[BuildResult] {

              //   val dataSource = json.parseJson.convertTo[DataSourceWrapper]
              dataSource.id match {
                case null =>
                  (server ? (Opt.CREATE, dataSource.copy(id = UUID.randomUUID().toString))).asInstanceOf[Future[BuildResult]]

                case _ =>
                  (server ? (Opt.UPDATE, dataSource)).asInstanceOf[Future[BuildResult]]

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

  }


  @ApiOperation(value = "检查配置合法性", notes = "检查配置合法性", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[DataSourceWrapper], paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "数据列表", response = classOf[List[DataSourceWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("pre-check")
  private def preCheck: Route = pathPrefix("pre-check") {

    entity(as[String]) {
      json =>
        val flag = try {
          mapper.readValue[DataSourceWrapper](json)
          true
        } catch {
          case e: Exception =>
            false
        }
        complete(s"""{"usable":$flag,"message":"上传文件不匹配"}""")

    }

  }

  @ApiOperation(value = "数据源预览", notes = "数据源预览", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "消息体", required = true,
      dataTypeClass = classOf[DataSourceWrapper], paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "数据列表", response = classOf[List[Map[String, Any]]]),
    new ApiResponse(code = 500, message = "Internal server error", response = classOf[Result])
  ))
  @Path("preview")
  private def datasourcePreview:Route = pathPrefix("preview"){
    parameter('pretty.?){
      pretty =>
        entity(as[String]) {
          json =>
            logger.info(s"PREVIEW datasource[$json]")
            val config = mapper.readValue[DataSourceWrapper](json)
            onComplete[(List[Map[String, Any]], _)] {

              logger.info(s"PREVIEW datasource[$config]")

              (server.?(Opt.PREVIEW, config)(previewTimeout)).asInstanceOf[Future[(List[Map[String, Any]], _)]]
            } {
              case Success(value) =>
                complete(toJson(value._1, pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

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
    new ApiResponse(code = 200, message = "分页的数据列表", response = classOf[Pagination[DataSourceWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def typeList: Route = pathPrefix("type"/Segment) {
    `type` =>
      parameter('pretty.?) {
        (pretty) =>
          onComplete[List[DataSourceWrapper]] {
            (server ? (Opt.GET, DataSourceType.withName(`type`))).asInstanceOf[Future[List[DataSourceWrapper]]]
          }
            {
            case Success(value) =>
              complete(toJson(value, pretty))
            case Failure(ex) =>
              ex.printStackTrace()
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))

          }
      }
  }
  @Path("type/{Segment}/{Segment}")
  @ApiOperation(value = "根据不同的类别查询通道列表(排除指定id)", notes = "根据不同的类别查询通道列表(排除指定id)", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "是否格式化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "type", value = "列表组合类别(channel,table,not-table或者default)", required = true,
      dataTypeClass = classOf[String], paramType = "path"),
    new ApiImplicitParam(name = "excludeIds", value = "需要排除的配置ID（多个配置逗号隔开）", required = true,
      dataTypeClass = classOf[String], paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "通道列表", response = classOf[Pagination[DataSourceWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  /**
    * DataSourceType.Channel : 包括解析通道、分析通道、数据表（单表+组合）、通道组合
    * DataSourceType.NotTable : 通道组合
    * DataSourceType.Table : 数据表（单表+组合）
    * DataSourceType.Default: 普通数据源（解析通道使用的数据源）
    * DataSourceType.ChannelWithoutTable ： 包括解析通道、分析通道、通道组合
    */
  private def typeListWithExclude: Route = pathPrefix("type"/Segment/Segment) {
    (`type`, `exclude`) =>
      parameter('pretty.?) {
        (pretty) =>
          onComplete[List[DataSourceWrapper]] {
            (server ? (Opt.GET, exclude, DataSourceType.withName(`type`))).asInstanceOf[Future[List[DataSourceWrapper]]]
          }
            {
              case Success(value) =>
                val excludes = `exclude`.split(",")
                complete(toJson(value.filter(d => !excludes.contains(d.id)), pretty))
              case Failure(ex) =>
                ex.printStackTrace()
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))

            }
      }
  }

  @ApiOperation(value = "获取数据通道配置", notes = "根据ID获取数据通道配置", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "结果是否美化", required = false,
      dataTypeClass = classOf[String], paramType = "query"),
    new ApiImplicitParam(name = "id", value = "开始位置", required = false,
      dataTypeClass = classOf[String], paramType = "path")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "通道配置", response = classOf[DataSourceWrapper]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("{id}")
  private def byId: Route = path(Segment) {
    id =>
      parameter('pretty.?) {
        pretty =>
          onComplete[Option[DataSourceWrapper]] {

            logger.info(s"query datasource[$id]")

            (server ? (Opt.GET, id)).asInstanceOf[Future[Option[DataSourceWrapper]]]
          } {
            case Success(value) =>
              value match {
                case Some(info) =>
                  complete(toJson(info, pretty))
                case _ =>
                  complete(toJson(BuildResult(StatusCodes.NotFound.intValue.toString, "113"), pretty))
                //complete(toJson(Result(StatusCodes.NotFound.intValue.toString, s"datasource[$id] NOT FOUND"), pretty))
              }


            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
            //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)
          }
      }
  }

    @ApiOperation(value = "删除", notes = "根据ID删除数据源", httpMethod = "delete")
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
      pathPrefix("datasource") {
        get {
          list ~ byId ~ typeListWithExclude ~ typeList
        } ~
          // create a datasource
          post {
            preCheck ~ addOrUpdate ~ datasourcePreview
          } ~
          _delete

      }
    }

  }


