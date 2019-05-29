package com.haima.sage.bigdata.etl.server.route

import java.util.Date
import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.TimeUtils
import io.swagger.annotations._

import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2015/6/18.
  */
@Api(value="Metric使用",produces="application/json")
@Path("/metrics/")
class MetricsRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {

  import context.dispatcher

  private lazy val server = context.actorSelection("/user/server")
  // metrics信息获取 URI:metrics/<collercorId>/<datasourceId>(or <writerId>)(必需)?mType=*&from=*&to=*
  @ApiOperation(value = "Metrics相关操作", notes="metrics相关操作",httpMethod="get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "mType", value = "Metric类型",required = false,
      dataTypeClass = classOf[String],paramType = "form"),
    new ApiImplicitParam(name = "mPhase", value = "",required = false,
      dataTypeClass = classOf[String],paramType = "form"),
    new ApiImplicitParam(name = "from", value = "",required = false,
      dataTypeClass = classOf[String],paramType = "form"),
    new ApiImplicitParam(name = "to", value = "",required = false,
      dataTypeClass = classOf[String],paramType = "form"),
    new ApiImplicitParam(name = "interval", value = "",required = false,
      dataTypeClass = classOf[String],paramType = "form"),
    new ApiImplicitParam(name = "pretty", value = "",required = false,
      dataTypeClass = classOf[String],paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Metrics列表", response = classOf[Pagination[MetricWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def metrics:Route =  path(Segment / Segment){
    (collectorId, configId) =>
      parameters('mType.?, 'mPhase.?, 'from.?, 'to.?, 'interval.?, 'pretty.?) {
        (mType, mPhase, from, to, interval, pretty) =>
          get {
            var fromVal: Date = null
            var toVal: Date = null
            // toVal 默认为currentDate
            to match {
              case Some(v) => toVal = TimeUtils.defaultParse(v)
              case None => toVal = TimeUtils.currentDate
            }
            // fromVal 默认为toVal之前的10分钟
            from match {
              case Some(v) => fromVal = TimeUtils.defaultParse(v)
              case None => fromVal = TimeUtils.minuteBefore(toVal, Constants.getApiServerConf("master.metrics.space-time").toInt)
            }
            if (fromVal.before(toVal)){
              onComplete[MetricWrapper]((server ? (collectorId, MetricQuery(configId,
                mType match {
                  case Some(metricType) => Some(MetricType.withName(metricType.toUpperCase))
                  case None => None
                },
                mPhase match {
                  case Some(metricPhase) => Some(MetricPhase.withName(metricPhase.toUpperCase))
                  case None => None
                }, fromVal, toVal))).map {
                case data: MetricWrapper@unchecked =>
                  data
                case _ =>
                  MetricWrapper(List[MetricInfo](),Map())
              }) {
                case Success(data) =>
                  complete(toJson(data, pretty))
                case Failure(ex) =>
                  complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
                //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

              }
            } else {
              logger.warn("错误的时间请求")
              complete(toJson(BuildResult(StatusCodes.BadRequest.intValue.toString, "914", "错误的时间请求"), pretty))
            }
          }
      }
  }


  def route:Route = {
    pathPrefix("metrics") {
      get {
        metrics
      }
    }
  }
}
