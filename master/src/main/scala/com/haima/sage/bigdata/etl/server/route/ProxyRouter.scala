package com.haima.sage.bigdata.etl.server.route


import akka.http.scaladsl.model.{HttpEntity, HttpRequest, Uri}
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.{Sink, Source}
import com.haima.sage.bigdata.etl.common.{Constants, Implicits}

/**
 * Created by zhhuiyan on 2015/6/30.
 */
trait ProxyRouter extends DefaultRouter {

  val proxy = {
    pathPrefix("proxy" / Segment) {

      serve => {


        entity(as[String]) {
          entityData => {
            ctx => {
              //TODO 需要根据当前用户token来获得endpoint列表，替换到
              val richMapVal1 = Implicits.richMap(mapper.readValue[Map[String, Any]](entityData))
              val richMapVal=richMapVal1.+("query.bool.filter"-> Map("terms" -> Map("endpoint" -> List("1", "2"))))
              val entityJsonString =mapper.writeValueAsString(richMapVal.toMap)
              this.logger.debug(entityJsonString)
              import com.haima.sage.bigdata.etl.server.Master._
              val host = Constants.MASTER.getString(s"web.proxy.$serve.host")
              val port = Constants.MASTER.getInt(s"web.proxy.$serve.port")
              val flow = Constants.MASTER.getString(s"web.proxy.$serve.protocol") match {
                case "http" =>
                  web.outgoingConnection(host, port)
                case "https" =>
                  web.outgoingConnectionHttps(host, port)
                case ptl =>
                  throw new NotImplementedError(s"unsupport protocol[$ptl]")
              }
              val req = ctx.request

              val url = req.uri.path.toString.replace(s"/proxy/$serve", "")
              this.logger.debug(s"proxy[$serve] to $url")

              val proxyReq = HttpRequest(method = req.method,
                uri = Uri.from(path = url).withQuery(req.uri.query()),
                entity = HttpEntity.apply(entityJsonString))
              //          web.singleRequest(proxyReq)
              Source.single(proxyReq)
                .via(flow)
                .runWith(Sink.head)
                .flatMap(ctx.complete(_))
            }
          }

        }
        //}

      }

    }
  }
}
