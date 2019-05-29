package com.haima.sage.bigdata.etl.metrics

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.{MetricTable, Opt}

/**
  * Created by zhhuiyan on 2017/4/20.
  */
object MetricRegistrySingle {
  private final var metric: MetricRegistry = null

  def apply(system: ActorSystem): MetricRegistry = {
    if (metric == null) metric = new MetricRegistryWithHistory(system)
    metric
  }

  def build(): MetricRegistry = metric
}

class MetricRegistryWithHistory(system: ActorSystem) extends MetricRegistry {

  private val watcher = system.actorSelection("/user/metric")

  import scala.concurrent.duration._
  import scala.collection.JavaConversions._
  import  system.dispatcher

  private val durationMinute = {
    CONF.getDuration(Constants.METRIC_INTERVAL_MINUTE).toMinutes minutes
  }
  system.scheduler.schedule(durationMinute, durationMinute) {
    this.getNames.foreach(watcher ! (Opt.FLUSH, MetricTable.METRIC_INFO_MINUTE, _))
  }
  private val durationHour = {
    CONF.getDuration(Constants.METRIC_INTERVAL_HOUR).toHours hours
  }
  system.scheduler.schedule(durationHour, durationHour) {
    this.getNames.foreach(watcher ! (Opt.FLUSH, MetricTable.METRIC_INFO_HOUR, _))
  }
  private val durationDay = {
    CONF.getDuration(Constants.METRIC_INTERVAL_DAY).toDays days
  }
  system.scheduler.schedule(0 seconds, durationDay) {
    this.getNames.foreach(watcher ! (Opt.FLUSH, MetricTable.METRIC_INFO_DAY, _))
    // 清理7天以前的计量信息
    watcher ! Opt.DELETE
  }

  private def flush(name: String): Unit = {
    watcher ! (Opt.FLUSH, name)
    watcher ! (Opt.FLUSH, MetricTable.METRIC_INFO_MINUTE, name)
    watcher ! (Opt.FLUSH, MetricTable.METRIC_INFO_HOUR, name)
    watcher ! (Opt.FLUSH, MetricTable.METRIC_INFO_DAY, name)
  }

  override def remove(name: String): Boolean = {
    val flag = super.remove(name)
    if (flag) flush(name)
    flag
  }
}

