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

package porter.app.akka.telnet

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.io.Tcp
import akka.util.{ByteString, Timeout}
import porter.BuildInfo
import porter.app.akka.PorterRef

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

private[telnet] class TelnetConnection(porter: PorterRef, conn: ActorRef) extends Actor {
  context.watch(conn)

  implicit val executor: ExecutionContextExecutor = context.dispatcher
  implicit val timeout = Timeout(5.seconds)

  private val ctrl_d = 4.toByte
  private val quit_requested = "quit requested"
  private val session = new Session(None)
  private val cmds = TelnetConnection.allCommands.reduce

  def receive = {
    case Tcp.Received(data) if data.utf8String.trim == "\\q" =>
      session.add(quit_requested, true)
      conn ! tcp("This will shutdown porter with all running services (telnet, http).\nReally? (yes/no): ")
    case Tcp.Received(data) if session.get(quit_requested).contains(true) =>
      if (data.utf8String.trim equalsIgnoreCase "yes") {
        context.system.terminate()
      } else {
        session.remove(quit_requested)
        conn ! prompt("")
      }

    case Tcp.Received(data) if data.utf8String.trim == "exit" || data.head == ctrl_d =>
      conn ! Tcp.Write(ByteString("Good bye.\n"))
      context.stop(self)

    case Tcp.Received(data) =>
      cmds(Input(data.utf8String.trim, conn, porter, session))

    case x: Tcp.ConnectionClosed =>
      context.stop(self)

    case Terminated(`conn`) =>
      context.stop(self)

    case Terminated(`porter`) =>
      conn ! Tcp.Write(ByteString("Porter actor terminated. Sorry, closing connection."))
      context.stop(self)
  }

  override def preStart(): Unit = {
    val welcome = prompt(
      s"""
        |Welcome to porter ${BuildInfo.version}
        |
        |Type 'help' for a list of available commands.
        |""".stripMargin)
    conn ! welcome
  }
}

object TelnetConnection {
  private[telnet] def props(porter: PorterRef, conn: ActorRef) = Props(new TelnetConnection(porter, conn))

  private val allCommands = HelperCommands ++ RealmCommands ++ AccountCommands ++ GroupCommands ++ AuthCommands
  val documentation: String = allCommands.makeDoc
}
