package com.haima.sage.bigdata.etl

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.util.Timeout

import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by zhhuiyan on 15/1/26.
 */
object FutureTest  {
  def main(args: Array[String]) {
    implicit val timeout = Timeout(5 seconds)
    val system = ActorSystem("AskTestSystem")
    val sender = system.actorOf(Props[Request], name = "myActor")
    val future = sender ? "start"

    println(Await.result(future, timeout.duration).asInstanceOf[String])
  }
}
class Request extends Actor{
override def receive: Receive = {
  case "start"=>
    sender() ! "dasdasdasdsad"
    context.actorOf(Props[Reply]) !"heng"
  case "ha"=>
    println("ha")
    TimeUnit.SECONDS.sleep(3)
    sender() !"heng"
  
}
}
class Reply extends Actor{
  implicit val timeout = Timeout(5 seconds)
  override def receive: Receive = {
    case "heng"=>
      println("heng")
    val future= sender ? "ha"
      try{
        println(Await.result(future, timeout.duration).asInstanceOf[String])
        
      }catch {
        case e:Exception=>
          e.printStackTrace()
        
      }
    
      sender ! "ha"
  }
}