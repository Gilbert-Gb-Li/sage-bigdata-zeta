/*
 * Copyright 2014 porter <https://github.com/eikek/porter>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package porter.runner

import java.net.InetSocketAddress

import akka.actor._
import akka.http.scaladsl.Http
import akka.io.{IO, Tcp}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import porter.app.akka.Porter
import porter.app.akka.http.{AuthService, StoreService}
import porter.app.akka.telnet.TelnetServer
import porter.runner.MainActor.{BindHttp, BindTelnet, ServiceBound, ServiceBoundFailure, _}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class MainActor extends Actor with ActorLogging {

  import akka.pattern.ask

  private implicit val timeout: Timeout = 3000.seconds
  private implicit val system = context.system
  implicit val executionContext: ExecutionContextExecutor = context.dispatcher

  val main = MainExt(context.system.asInstanceOf[ExtendedActorSystem])
  // val openidSettings = OpenIdSettings(context.system)

  val porter: ActorRef = Porter(context.system).fromSubConfig(context)
  val telnetService: ActorRef = context.actorOf(TelnetServer(porter), "telnet")
  // val httpService  : ActorRef = context.actorOf(HttpHandler(porter, main.http.decider), "http")
  // val openIdService: ActorRef = context.actorOf(OpenIdHandler(porter, openidSettings), "openid")

  private def toTcp(actor: ActorRef, host: String, port: Int): Future[Any] = {
    val endpoint = new InetSocketAddress(host, port)
    IO(Tcp) ? Tcp.Bind(actor, endpoint)
  }


  private def bind(name: String, bind: => Future[Any]) {
    def mapBindResponse(f: Future[Any]): Future[Any] = {
      f.map({
        case ok: Tcp.Bound => ServiceBound(name, ok)
        case f: Tcp.CommandFailed => ServiceBoundFailure(name, new Exception(f.cmd.toString))
        case f@_ => ServiceBoundFailure(name, new Exception(s"Unexpected response to bind: $f"))
      }).recover({ case x => ServiceBoundFailure(name, x) })
    }

    mapBindResponse(bind)
  }

  def receive: Receive = {
    case BindTelnet(host, port) =>
      bind("telnet", toTcp(telnetService, host, port))

    case BindHttp(_host, port) =>
      val authRoute = AuthService(porter, main.http.decider).route
      val storeRoute = StoreService(porter, main.http.decider).route
      implicit val materializer = ActorMaterializer()



      import akka.http.scaladsl.server.Directives._
      Http(system).bindAndHandle(authRoute ~ storeRoute, interface = _host, port = port)

    /*case BindOpenid(host, port) =>
      bind("openid", toHttp(openIdService, host, port))*/

    case b@ServiceBound(name, addr) =>
      println(s"--- Bound $name to ${addr.localAddress}")

    case ServiceBoundFailure(name, ex) =>
      log.error(ex, s"Binding $name failed")
      system.terminate()
  }


  override def preStart(): Unit = {
    def bindService(bind: ServiceBind) {
      (self ? bind).mapTo[ServiceBound]
    }

    if (main.telnet.enabled) {
      bindService(BindTelnet(main.telnet.host, main.telnet.port))
    }
    if (main.http.iface.enabled) {
      bindService(BindHttp(main.http.iface.host, main.http.iface.port))
    }
    /* if (main.openid.enabled) {
       bindService(BindOpenid(main.openid.host, main.openid.port))
     }*/
  }
}

object MainActor {

  def apply() = Props(classOf[MainActor])

  sealed trait ServiceBind {
    def host: String

    def port: Int
  }

  case class BindHttp(host: String, port: Int) extends ServiceBind

  case class BindTelnet(host: String, port: Int) extends ServiceBind

  case class BindOpenid(host: String, port: Int) extends ServiceBind

  case class ServiceBound(name: String, bount: Tcp.Bound)

  case class ServiceBoundFailure(name: String, ex: Throwable)

}
