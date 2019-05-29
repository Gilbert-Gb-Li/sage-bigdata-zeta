package com.haima.sage.bigdata.etl.server

/**
  * Created by zhhuiyan on 15/11/23.
  */

import akka.actor._

import  com.haima.sage.bigdata.etl.common.Constants.executor

object WatchTest extends App {

  // create the ActorSystem instance
  val system = ActorSystem("DeathWatchTest")

  // create the Parent that will create Kenny
  val parent = system.actorOf(Props[Parent], name = "Parent")
  val watcher = system.actorOf(Props[Watchers], name = "Watchers")

  parent ! "create"
  // lookup kenny, then kill it

  val kenny = system.actorSelection("/user/Parent/Kenny")
  kenny ! "hahah"

  watcher ! "watch"
  Thread.sleep(2000)
  kenny ! PoisonPill
  Thread.sleep(2000)
  parent ! "create"
  Thread.sleep(2000)
  kenny ! PoisonPill
  Thread.sleep(2000)
  parent ! "create"
  Thread.sleep(2000)
  kenny ! PoisonPill
  Thread.sleep(2000)
  //  system.shutdown()


}

class Kenny extends Actor {
  def receive = {
    case (msg: String) => println(s"Kenny received a message:$msg")
    case _ => println("Kenny received a no text message")
  }
}

class Watchers extends Actor {
  override def receive: Receive = {
    case ActorIdentity("1", Some(actor)) =>
      println(s"Start to watch the worker[${actor.path}].")
      context.watch(actor)
    case ActorIdentity("1", None) =>
      println(s"Identify None.")
      import scala.concurrent.duration._
      context.system.scheduler.scheduleOnce(1000 millisecond) {
        context.actorSelection("/user/Parent/Kenny") ! Identify("1")
      }

    case "watch" =>
      context.actorSelection("/user/Parent/Kenny") ! Identify("1")
      println(s"Identify to watch the worker[/user/Parent/Kenny].")
    case Terminated(kenny) =>
      println(s"OMG, they killed Kenny[$kenny]")
      try {
        /*val actor2 = context.actorOf(Props[Kenny], name = "Kenny")*/
        context.actorSelection("/user/Parent/Kenny") ! Identify("1")

      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    case obj => println(s"Watchers received a message:$obj")
  }
}

class Parent extends Actor {
  // start Kenny as a child, then keep an eye on it


  def receive = {
    case ("create") =>

      println(s"create,[${context.actorOf(Props[Kenny], name = "Kenny")}]")

    case _ => println("Parent received a message")
  }
}
