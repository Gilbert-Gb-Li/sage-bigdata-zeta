package com.haima.sage.bigdata.etl.server


import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}

import akka.actor.{ActorSystem, Props}
import akka.cluster.client.ClusterClientReceptionist
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.Opt
import com.haima.sage.bigdata.etl.server.hander.{CollectorServer, RunStatusServer, TaskManagerServer}
import com.haima.sage.bigdata.etl.server.master.{ClusterListener, Watcher}
import com.haima.sage.bigdata.etl.server.route.Router
import com.haima.sage.bigdata.etl.utils.Logger
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 15/11/23.
  */
object Master extends App with Router with SSLConfiguration {
  //implicit val timeout = Timeout(Constants.getApiServerConf(Constants.MASTER_SERVICE_TIMEOUT).toLong, java.util.concurrent.TimeUnit.SECONDS)
  Logger.init("logback-master.xml")
  Constants.init("master.conf")
 // private val conf = ConfigFactory.load()
  implicit val context: ActorSystem = ActorSystem("master", Constants.CONF.resolve())
  context.actorOf(Props[ClusterListener], name = "listener")
  implicit val materializer = ActorMaterializer()

  implicit val executionContext: ExecutionContextExecutor = context.dispatcher


  /*               system.actorOf(Props[ClusterListener], name = "listener")

   startup(Seq("2551", "2552"))*/

  // api DB store

  // a factory used to store the actorRef of each worker in the zeta system
  private val watcher = context.actorOf(Props[Watcher].withDispatcher("akka.dispatcher.server"), "watcher")
  private val server = context.actorOf(Props[master.Server].withDispatcher("akka.dispatcher.server"), "server")
  /*采集器服务要做成全局- 其他的服务可以访问,但是不需要注册到外部可访问*/
  context.actorOf(Props[CollectorServer].withDispatcher("akka.dispatcher.server"), "collector-server")
  val receptionist = ClusterClientReceptionist(context)
  receptionist.registerService(server)
  receptionist.registerService(watcher)
  receptionist.registerService(context.actorOf(Props[master.MetricServer].withDispatcher("akka.dispatcher.server"), "metric"))
  /*状态服务要做成全局-worker 可以访问*/
  receptionist.registerService(context.actorOf(Props[RunStatusServer].withDispatcher("akka.dispatcher.server"), "status-server"))
  context.actorOf(Props[TaskManagerServer], "timerTaskServer") ! (Opt.SYNC, Opt.INITIAL)
  /*final val syncServer: ActorRef = context.actorOf(Props[InitSyncServer], "sync-server")*/
  // the server used to provide the RESTFul api service
  //val service: ActorRef = system.actorOf(Props[Router], "router")

  /*ClusterReceptionistExtension(system).registerService(server)*/
  // ClusterReceptionistExtension(system).registerService(server)
  // an actor used to start task
  //DBCreateUtils.create()



  // 生成Swagger UI 需要的数据
//  if (Constants.MASTER.getBoolean("web.http.enable")) {
//    val host = Constants.MASTER.getString("web.http.host")
//    val port = Constants.MASTER.getInt("web.http.port")
//    val SwaggerSpec = new OutputStreamWriter(new FileOutputStream("zeta-master/src/main/resources/api/SwaggerSpec.json"))
//    //val swaggerSpecWeb = web.singleRequest(HttpRequest(uri = s"http://$host:$port/api-docs/swagger.json"))
//    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = s"http://$host:$port/api-docs/swagger.json", method = HttpMethods.GET))
//    responseFuture.onComplete {
//      case Success(res) => {
//        println(res)
//        Unmarshal(res.entity).to[String].map { json_str =>
//          Right {
//            SwaggerSpec.write(json_str)
//            SwaggerSpec.close()
//          }
//        }
//      }
//      case Failure(error) => println(error.getMessage)
//    }
//    //logger.debug("SwaggerSpec"+swaggerSpec.toString)
//  }


  final val web = Http(context)

  val routes: Flow[HttpRequest, HttpResponse, Any] = route()

  if (Constants.MASTER.getBoolean("web.https.enable")) {
    val host = Constants.MASTER.getString("web.https.host")
    val port = Constants.MASTER.getInt("web.https.port")
    web.bindAndHandle(routes, host, port, connectionContext = ConnectionContext.https(sslContext))
    logger.debug(s"Server(https) online at https://$host:$port/ui/index.html")
  }

  if (Constants.MASTER.getBoolean("web.http.enable")) {
    val host = Constants.MASTER.getString("web.http.host")
    val port = Constants.MASTER.getInt("web.http.port")
    web.bindAndHandle(routes, host, port.toInt)
    logger.debug(s"Server(http) online at http://$host:$port/ui/index.html")
  }





  /*val bindingFuture = http.bindAndHandle(r,
    s"${Constants.getApiServerConf(Constants.MASTER_SERVICE_BIND_IP)}"
    , Constants.getApiServerConf(Constants.MASTER_SERVICE_BIND_PORT).toInt)

  println(s"Server(http) online at http://${Constants.getApiServerConf(Constants.MASTER_SERVICE_BIND_IP)}:${Constants.getApiServerConf(Constants.MASTER_SERVICE_BIND_PORT).toInt}/ui/index.html")
  println()


  bindingFuture.onComplete(d => {
    println(d)
    //context.terminate()
  })*/
  // and shutdown when done
  // start api server
  /*(IO(Http) ? Http.Bind(service,
    s"${Constants.getApiServerConf(Constants.MASTER_SERVICE_BIND_IP)}",
    port = Constants.getApiServerConf(Constants.MASTER_SERVICE_BIND_PORT).toInt,
    settings = Some(ServerSettings(ConfigFactory.load(Constants.MASTER_API))))).onComplete{
    case Success(Bound(info))=>
    case _=>

  }*/

  // listen to DeadLetter
  // listen to Remote Actor Lifecycle
  //context.eventStream.subscribe(server, classOf[akka.remote.RemotingLifecycleEvent])

  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>
      // Override the configuration of the port
      val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port).
        withFallback(ConfigFactory.load(Constants.CONF))

      // Create an Akka system
      val system = ActorSystem("master", config)
      // Create an actor that handles cluster domain events
      system.actorOf(Props[ClusterListener], name = "listener")

    }
  }






  // override implicit val context: ActorSystem = system

}
