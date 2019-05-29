package com.haima.sage.bigdata.etl.utils

import java.util.concurrent.Executors

import akka.actor.{Props, Actor, ActorRef}
import com.haima.sage.bigdata.etl.common.model.Opt


import scala.collection.mutable

/**
 * Created by zhhuiyan on 15/2/12.
 */
class FlowOpt extends Actor with Logger{
  val pool = Executors.newCachedThreadPool()

      class Push extends Actor{
        override def receive: Actor.Receive = {
          case (name: String, n: Int) =>
            data.put(name, data.getOrElse(name, 0l) + n)
            context.stop(self)
        }
      }
  class Get extends Actor{
    override def receive: Actor.Receive = {
      case (from:ActorRef, Opt.GET) =>
        val n = if (data.isEmpty) {
          0l
        } else {
          val count = data.values.min
          data.foreach {
            case (key, value) =>
              data.put(key, value - count)
          }
          count
        }
        from ! n
        context.stop(self)
    }
  }
  val data: mutable.Map[String, Long] = mutable.Map()


  override def receive: Receive = {
    case (name: String, n: Int) =>
      context.actorOf(Props(classOf[Push],this ))!(name, n)
/*
      pool.execute(new Runnable {
        override def run(): Unit = {
          val now = System.currentTimeMillis()
          data.put(name, data.getOrElse(name, 0l) + n)
          logger.debug(s"push take ${System.currentTimeMillis() - now} ms")
        }
      })*/

    case Opt.GET =>

      context.actorOf(Props(classOf[Get],this ))!(sender(), Opt.GET)
      /*val from=sender()
      pool.execute(new Runnable {
        override def run(): Unit = {
          val now = System.currentTimeMillis()
          val n = if (data.isEmpty) {
            0l
          } else {
            val count = data.values.min
            data.foreach {
              case (key, value) =>
                data.put(key, value - count)
            }
            count
          }
          logger.debug(s"get take ${System.currentTimeMillis() - now} ms")
          from ! n
        }
      })*/

  }
}



