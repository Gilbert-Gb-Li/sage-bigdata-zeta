package com.haima.sage.bigdata.etl.server.worker

import java.io.IOException
import java.util

import akka.actor.{Actor, ActorRef, Props}
import com.haima.sage.bigdata.etl.common.Constants.{CONF, STORE_CACHE_CLASS, STORE_CACHE_CLASS_DEFAULT}
import com.haima.sage.bigdata.etl.common.model.Opt
import com.haima.sage.bigdata.etl.store.CacheStore
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by zhhuiyan on 15/1/29.
  */

class CacheService(store: CacheStore) extends Actor {
  override def receive: Actor.Receive = {
    case (key: String, log: Map[String@unchecked, Any@unchecked]) =>
      store.put(key, log)
      context.stop(self)
  }
}

class CacheServer extends Actor {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[CacheServer])
  private val store: CacheStore = try {
    val clazz: Class[CacheStore] = Class.forName(CONF.getString(STORE_CACHE_CLASS) match {
      case null =>
        STORE_CACHE_CLASS_DEFAULT
      case other =>
        other
    }).asInstanceOf[Class[CacheStore]]
    clazz.newInstance
  } catch {
    case e: IOException =>
      logger.error(s"cannot init read position storer error:$e ")
      throw new IOException("cannot init read position storer  ")
  }
  private val exes = new util.LinkedHashMap[String, StreamThread]()

  override def receive: Receive = {
    case (key: String, Opt.GET) =>
      if (!exes.containsKey(key)) {
        val stream = new StreamThread(sender(), key)
        exes.put(key, stream)
        stream.start()
      }
    case (key: String, Opt.STOP) =>
      if (!exes.containsKey(key)) {
        sender() ! (key, Opt.STOP)
      } else {
        exes.get(key).kill()
        import  context.dispatcher
        import scala.concurrent.duration._
        context.system.scheduler.scheduleOnce(1 seconds, new Runnable {
          override def run(): Unit = {
            if (exes.containsKey(key)) {
              sender() ! Opt.STOP
            } else {
              sender() ! (key, Opt.STOP)
            }

          }
        })
      }


    case (key: String, Opt.DELETE) =>
      val stream = exes.get(key)
      if (stream != null)
        stream.kill()
    case (key: String, log: Map[String@unchecked, Any@unchecked]) =>
      context.actorOf(Props(classOf[CacheService], store)) ! (key, log)
    case msg =>

      logger.warn(s"unknown message:$msg ")
  }

  class StreamThread(from: ActorRef, key: String) extends Thread {

    var running = true

    override def run(): Unit = {
      val stream = store.getById(key)
      try {
        for (log <- stream if running) {
          from ! log
        }
      } finally {
        stream.close()
      }
      store.remove(key)
      exes.remove(key)
    }

    def kill(): Unit = {
      running = false
    }

  }

}
