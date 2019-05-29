package com.haima.sage.bigdata.etl.server.route

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Sink, Source}
import com.haima.sage.bigdata.etl.server.swagger.SwaggerDocService

import scala.concurrent.Await
import scala.util.Try

/**
  * Created by zhhuiyan on 2015/1/30.
  */
//with SecurityDomainAPI
//with DictionaryPropertiesAPI with SparyPathAPI

trait Router extends RulesRouter
  with ProxyRouter with AuthorityRouter with HealthRouter {


  /*with AccountRouter with SignRouter*/

  def notFound: Route = { ctx =>
    logger.debug(s"not found  path" + ctx.unmatchedPath)
    redirect(Uri("/ui/index.html"), SeeOther).apply(ctx)
  }


  implicit def myRejectionHandler: RejectionHandler =
    RejectionHandler.newBuilder()
      .handle {
        case MissingCookieRejection(cookieName) =>
          complete(HttpResponse(BadRequest, entity = "No cookies, no service!!!"))
        case AuthorizationFailedRejection =>
          complete((Forbidden, "You're out of your depth!"))
        case ValidationRejection(msg, _) =>
          complete((InternalServerError, "That wasn't valid! " + msg))
      }.handleAll[MethodRejection] { methodRejections =>
      val names = methodRejections.map(_.supported.name)
      complete((MethodNotAllowed, s"Can't do that! Supported: ${names mkString " or "}!"))
    }.handleNotFound {
      //notFound
      redirect(Uri("/ui/index.html"), SeeOther)
    }.result()


  private[Router] lazy val path = auths.getString("authority.validate.path")
  private[Router] lazy val has = Try(auths.getBoolean("authority.validate.enable")).getOrElse(false)


  def ignore(implicit url: String): Boolean = {
    url.equals("") ||
      url.equals("/") ||
      url.equals("/favicon.ico") ||
      url.startsWith("/ui") || url.startsWith("/api") || url.startsWith("/api-docs")
  }

  def route(): Route = {


    SwaggerDocService.routes ~ auth ~ HtmlRouter.route ~ authorize {
      request => {

        val url = request.request.uri.path.toString()
        if (accessCheck && !ignore(url)) {
          if (has) {
            import com.haima.sage.bigdata.etl.server.Master.{executionContext, materializer}
            //   val proxyReq = HttpRequest(method = request.request.method, uri = Uri.from(path = s"$path"), entity = request.request.entity)

            val method = request.request.method.value.toLowerCase

            val token = request.request.headers.find(hd => hd.is("token")) match {
              case Some(header) =>
                header.value()
              case _ =>
                "custom"
            }
            val currentUserid = request.request.headers.find(hd => hd.is("currentuserid")) match {
              case Some(header) =>
                header.value()
              case _ =>
                "1"
            }

            val entity = s"""{ "realm": "zeta", "account":"$token","perms":["resource:$method:$url"]}""".stripMargin
            val uri = Uri.from(path = path)
            // val proxyReq = HttpRequest(method =HttpMethods.POST, uri = Uri.from(path = s"$path"), entity = HttpEntity(ContentTypes.`application/json`,entity))

            //            val entity = s"""{ "Token": $token, "UserId":1}""".stripMargin

            //            val proxyReq = HttpRequest(method = request.request.method, uri = Uri.from(path = s"$path", host = "10.10.106.168", port = 9000))

            val proxyReq = HttpRequest(method = HttpMethods.POST,
              uri = uri, entity = HttpEntity(ContentTypes.`application/json`, entity))
            //              uri = Uri.from(host = host, port = port, path = path), entity = request.request.entity.withContentType(ContentTypes.`application/json`))
            //              .withEntity(HttpEntities.create(ContentTypes.`application/json`, "{\"ucode\":\"superadmin\",\"password\":\"123456\"}"))

            Try(Await.result[HttpResponse](Source.single(proxyReq).via(flow).runWith(Sink.head), timeout.duration)) match {
              case scala.util.Success(response) =>
                val result = response.status match {
                  case OK =>
                    val data = Await.result[Map[String, Any]](Unmarshal(response.entity).to[String]
                      .map {
                        jsonString => mapper.readValue[Map[String, Any]](jsonString)
                      },
                      timeout.duration)
                    //true
                    data.get("authorized") match {
                      case Some(true) =>
                        true
                      case Some("true") =>
                    true
                      case _ =>
                        false
                    }

                  case BadRequest =>
                    logger.debug(s"BadRequest:${response.status}->${response.entity}")
                    false
                  case status =>
                    logger.debug(s"$status->${response.entity}")
                    false
                }
                logger.debug(s"auth for resource:$method:$url:$result")
                result


              case ddd =>
                logger.debug(s" to $url fail:$ddd")
                false
            }
          } else {
            /*TODO NOT implement*/
            false
          }


        } else {
          //                logger.debug("NOT USE VALID")
          true
        }
      }


    } {

      /*account ~*/
      new KnowledgeRouter(context).route ~ new CollectorRouter(context).route ~ new ConfigRouter(context).route ~ health ~ new MetricsRouter(context).route ~ rules ~ new PreviewRouter(context).route ~ new AnalyzerRouter(context).route ~
        new ParserRouter(context).route ~ new WriterRouter(context).route ~ new TaskManagerRouter(context).route ~ new DictionaryRouter(context).route ~ new DataSourceRouter(context).route ~ new ConfigUsabilityRouter(context).route ~ proxy ~ new ModelingRouter(context).route ~new KnowledgeInfoRouter(context).route
    }
    /*account ~ parser ~ collector ~ config ~ health ~ metrics ~ rules ~ preview ~
     parser ~ assetType ~ assets ~ writer ~ dictionary ~ datasource ~ usability ~ proxy*/
  }


}







