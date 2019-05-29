package com.haima.sage.bigdata.etl.server.route

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.{Sink, Source}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.server.Master.web

import scala.util.Try


trait AuthorityRouter extends DefaultRouter {


  private[route] lazy val auths = Constants.MASTER.getConfig("web.auth")
  private[route] lazy val protocol = auths.getString("protocol")
  private[route] lazy val host = auths.getString("host")
  private[route] lazy val port = auths.getInt("port")
  private lazy val signIn = auths.getString("sign-in")
  private lazy val signOUt = auths.getString("sign-out")
  private lazy val authorities = auths.getString("authority.all")
  private[route] lazy val flow = protocol match {
    case "http" =>
      logger.debug(s"auth server in http://$host:$port")
      web.outgoingConnection(host, port)
    case "https" =>
      web.outgoingConnectionHttps(host, port)
    case ptl =>
      throw new NotImplementedError(s"unsupport protocol[$ptl]")
  }
  private[route] lazy val accessCheck: Boolean = Try(Constants.MASTER.getBoolean("web.auth.enable")).getOrElse(false)

  def auth = {

    parameter('pretty.?) {
      import com.haima.sage.bigdata.etl.server.Master.{executionContext, materializer}
      pretty => {
        path("sign-in") {
          ctx =>
            if (accessCheck) {
              /*http://10.20.32.70:9000/authority/load*/

              val req = ctx.request
              //            val proxyReq = HttpRequest(method = req.method, uri = Uri.from(path = s"http://$host:$port$signIn"), entity = req.entity)

              val uri = Uri.from(path = signIn)

              logger.debug(s"uri:$uri")
              logger.debug(s"req:${req.entity}")
              logger.debug(s"method:${req.method}")


              val proxyReq = HttpRequest(
                method = req.method,
                uri = uri, entity = HttpEntity(ContentTypes.`application/json`, req.entity.dataBytes))
              //              .withEntity(HttpEntities.create(ContentTypes.`application/json`, "{\"ucode\":\"superadmin\",\"password\":\"123456\"}"))

              Source.single(proxyReq)
                .via(flow)
                .runWith(Sink.head)
                .flatMap {
                  data =>
                    logger.debug("data:" + data)

                    ctx.complete(data)
                }
            } else {
              logger.debug(s"login with:" + ctx.request.entity)
              ctx.complete(HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`),
                s"""{
                   |  "success":true,
                   |  "account":{
                   |    "id":1,
                   |    "name":"anonymous",
                   |    "props":{
                   |      "email":"anonymous@haima.me"
                   |     }
                   |  }
                   |}""".stripMargin)))
            }

        } ~ path("sign-out") { ctx =>


          val req = ctx.request
          val proxyReq = HttpRequest(method = req.method, uri = Uri.from(path = s"$signOUt"), entity = req.entity)

          Source.single(proxyReq)
            .via(flow)
            .runWith(Sink.head)
            .flatMap(ctx.complete(_))

        } ~ path("authority") {
          path("validate") {
            complete("{validate:true}")
          } ~ {
            ctx =>
              val req = ctx.request
              val proxyReq = HttpRequest(method = req.method, uri = Uri.from(path = s"$authorities"), entity = req.entity)

              Source.single(proxyReq)
                .via(flow)
                .runWith(Sink.head)
                .flatMap(ctx.complete(_))
          }


        }
      }
    }

  }
}
