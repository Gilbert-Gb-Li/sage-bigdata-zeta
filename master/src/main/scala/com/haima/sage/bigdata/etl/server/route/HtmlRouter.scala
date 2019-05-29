package com.haima.sage.bigdata.etl.server.route

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

/**
  * Created by zhhuiyan on 2015/6/30.
  */
object HtmlRouter {

  def route: Route = {
    pathPrefix("api") {
      get {
        getFromResourceDirectory("api")
      }
    } ~
      pathPrefix("ui") {
        get {
          getFromResourceDirectory("ui")
        }
      }
  }
}
