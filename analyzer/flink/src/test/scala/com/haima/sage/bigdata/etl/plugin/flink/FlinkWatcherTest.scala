package com.haima.sage.bigdata.etl.plugin.flink

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model.FlinkAPIs
import org.junit.{After, Before, Test}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by evan on 17-11-6.
  */
class FlinkWatcherTest {

  var system: ActorSystem = _
  var watcher: ActorRef = _
  implicit val timeout = Timeout(1 minutes)

  @Before
  def before(): Unit = {
    system = ActorSystem("worker")
    watcher = system.actorOf(Props.create(classOf[FlinkWatcher], "http://zdp02.asdas.com:8088/proxy/application_1506578438298_0001"), "watcher")
  }

  @Test
  def cluster_overview(): Unit = {
    assert(!Await.result(watcher ? FlinkAPIs.CLUSTER_OVERVIEW, timeout.duration).asInstanceOf[Map[String, Any]].contains("error"))
  }

  @Test
  def slots_available(): Unit = {
    assert(Await.result(watcher ? FlinkAPIs.SLOTS_AVAILABLE, timeout.duration).asInstanceOf[Int] > 0)
  }

  @Test
  def jobs_overview(): Unit = {
    assert(!Await.result(watcher ? FlinkAPIs.JOBS_OVERVIEW, timeout.duration).asInstanceOf[Map[String, Any]].contains("error"))
  }

  @Test
  def job_manager_config(): Unit = {
    assert(!Await.result(watcher ? FlinkAPIs.JOB_MANAGER_CONFIG, timeout.duration).asInstanceOf[Map[String, Any]].contains("error"))
  }

  /*@Test
  def job_metrics(): Unit ={
    println(Await.result(watcher ? ("7e78da0ca4bed55246cdc90d5b261b81", FlinkAPIs.JOB_METRICS), timeout.duration).asInstanceOf[Map[String, Any]])
  }*/

  @After
  def after(): Unit = {
    system.stop(watcher)
    system.terminate()
  }
}
