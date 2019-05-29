package com.haima.sage.bigdata.etl.server.route

import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, MediaTypes}
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{BuildResult, Result}
import com.haima.sage.bigdata.etl.utils.{Logger, Mapper}
import com.typesafe.config.ConfigFactory

/**
  * Created by zhhuiyan on 2015/6/18.
  */
trait DefaultRouter extends Logger with Mapper {

  implicit lazy val timeout = Timeout(Constants.getApiServerConf(Constants.MASTER_SERVICE_TIMEOUT).toLong, java.util.concurrent.TimeUnit.SECONDS)
  protected lazy val previewTimeout = Timeout(15, java.util.concurrent.TimeUnit.SECONDS)
  implicit val context : akka.actor.ActorSystem

  lazy val data = ConfigFactory.load("prompts.conf")

  def toJson(value: AnyRef, pretty: Option[String]) =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), pretty match {
      case Some(_) =>
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
      case None =>
        value match {
          case d: BuildResult =>
            mapper.writeValueAsString(buildResult(d))
          case _ =>
            mapper.writeValueAsString(value)
        }
    }))

  def buildResult(value: BuildResult): Result = {
    val message = data.getString(value.key) match {
      case d:String =>
        if(!d.isEmpty){
          var result = d
          var i = 0
          if(value.param.length == 0){
            result = result.replaceAll(" \\[\\{[0-9]\\}\\] ","").replace("：","")
          }
          else{
            for (i <- 0 to (value.param.length-1)){
              if(value.param(i).isEmpty){
                result=result.replace("：[{"+i+"}] ","")
              }
              else{
                result = result.replace("{"+i+"}",value.param(i))
              }
            }
            result = result.replaceAll("(：)*\\[\\{[0-9]\\}\\] ","")
          }
          result
        }
        else{
          ""
        }
      case _=>
        ""
    }
    Result(value.status, message.toString)
  }
}