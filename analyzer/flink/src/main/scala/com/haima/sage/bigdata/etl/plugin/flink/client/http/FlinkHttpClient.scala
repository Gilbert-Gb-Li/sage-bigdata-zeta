package com.haima.sage.bigdata.etl.plugin.flink.client.http

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.haima.sage.bigdata.etl.common.model.Opt
import com.haima.sage.bigdata.etl.utils.Logger

/**
  * Created by evan on 17-10-26.
  */
class FlinkHttpClient extends Actor with Logger {

  import akka.pattern.pipe
  import context.dispatcher

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
  val http = Http(context.system)

  def receive: PartialFunction[Any, Unit] = {
    case request: HttpRequest =>
      http.singleRequest(request).pipeTo(sender())
    case Opt.STOP =>
      context.stop(self)
  }
}
