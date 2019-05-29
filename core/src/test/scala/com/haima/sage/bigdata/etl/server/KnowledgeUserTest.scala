package com.haima.sage.bigdata.etl.server

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.{Opt, RichMap}
import org.junit.Test

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class KnowledgeUserTest {
  Constants.init("worker.conf")
  final val system = ActorSystem("worker", CONF.resolve())

  @Test
  def loadsyncActor(): Unit = {
    val actor = system.actorOf(Props(new Actor {
      private lazy val cache = (0 to 1000000).map(i => {
        (i.toString, Map("gift_key" -> i.toString, "gift_unit_val" -> Math.random() * 10))
      }).toMap

      private lazy val loadingCache: LoadingCache[String, Map[String, Any]] =
        Caffeine.newBuilder.maximumSize(1000000).initialCapacity(1000000).build(new CacheLoader[String, Map[String, Any]] {


          override def load(k: String): Map[String, Any] = {
            RichMap()
            // cache.getOrElse(k,RichMap())
          }
        })


      override def preStart(): Unit = {
        import scala.collection.JavaConverters._
        loadingCache.putAll(cache.asJava)
      }

      override def receive: Receive = {
        case (Opt.GET, key) =>
          sender() ! loadingCache.get(key.toString)


      }
    }))

    implicit val timeout = Timeout(5 minutes)

    val start = System.currentTimeMillis()
    val n = 500000
    (0 to n).foreach(i => {
      Await.result[RichMap]((actor ? (Opt.GET, (101 + i).toString)).asInstanceOf[Future[RichMap]], Duration.Inf).getOrElse("gift_unit_val", "-1").toString.toDouble
      Await.result[RichMap]((actor ? (Opt.GET, "a" + (101 + i).toString)).asInstanceOf[Future[RichMap]], Duration.Inf).getOrElse("gift_unit_val", "-1").toString.toDouble
    })
    println(s"find $n take ${System.currentTimeMillis() - start} ms")

    system.terminate()
  }

  @Test
  def loadActor(): Unit = {
    val actor = system.actorOf(Props(new Actor {
      private lazy val cache = (0 to 1000000).map(i => {
        (i.toString, Map("gift_key" -> i.toString, "gift_unit_val" -> Math.random() * 10))
      }).toMap

      private lazy val loadingCache: LoadingCache[String, Map[String, Any]] =
        Caffeine.newBuilder.maximumSize(1000000).initialCapacity(1000000).build(new CacheLoader[String, Map[String, Any]] {


          override def load(k: String): Map[String, Any] = {
            RichMap()
            // cache.getOrElse(k,RichMap())
          }
        })


      override def preStart(): Unit = {
        import scala.collection.JavaConverters._
        loadingCache.putAll(cache.asJava)
      }

      override def receive: Receive = {
        case (Opt.GET, key) =>
          sender() ! loadingCache.get(key.toString)


      }
    }))

    implicit val timeout = Timeout(5 minutes)

    val start = System.currentTimeMillis()
    val n = 1000000


    implicit val dispatcher = system.dispatcher
    val futures = (0 to n).map(i => {

      (actor ? (Opt.GET, (101 + i).toString)).asInstanceOf[Future[Map[String, Any]]]
    })


    while (!futures.forall(_.isCompleted)) {
      TimeUnit.MICROSECONDS.sleep(1)
    }
    println(s"find $n take ${System.currentTimeMillis() - start} ms")

    system.terminate()
  }

  @Test
  def loadTest(): Unit = {
    val cache = (0 to 1000000).map(i => {
      (i.toString, Map("gift_key" -> i.toString, "gift_unit_val" -> Math.random() * 10))
    }).toMap
    val loadingCache: LoadingCache[String, Map[String, Any]] =
      Caffeine.newBuilder.maximumSize(1000000).initialCapacity(1000000).build(new CacheLoader[String, Map[String, Any]] {


        override def load(k: String): Map[String, Any] = {
          RichMap()
          // cache.getOrElse(k,RichMap())
        }
      })
    import scala.collection.JavaConverters._
    loadingCache.putAll(cache.asJava)

    val start = System.currentTimeMillis()
    val n = 1000000

    (0 to n).foreach(i => {
      loadingCache.get((101 + i).toString).getOrElse("gift_unit_val", "-1").toString.toDouble

    })
    println(s"find $n take ${System.currentTimeMillis() - start} ms")
  }

}
