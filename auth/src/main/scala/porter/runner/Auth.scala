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

import akka.actor._
import com.haima.sage.bigdata.etl.utils.Logger
import com.typesafe.config.ConfigFactory

import scala.util.control.NonFatal

/**
 * Starts the [[porter.runner.MainActor]]
 */
object Auth {

  val name = "porter"

  def main(args: Array[String]): Unit = {
    if (args.length != 0) {
      println("This app takes no arguments. They are ignored.")
    }
    Logger.init("logback-auth.xml")
    val system = ActorSystem(name, ConfigFactory.load("auth.conf"))
    try {
      val app = system.actorOf(MainActor(), name)
      system.actorOf(Props(classOf[Terminator], app), name+"-terminator")
    } catch {
      case NonFatal(e) ⇒ system.terminate(); throw e
    }
  }

  class Terminator(app: ActorRef) extends Actor with ActorLogging {
    context watch app
    def receive = {
      case Terminated(_) ⇒
        log.info("application supervisor has terminated, shutting down")
        context.system.terminate()
    }
  }

}
