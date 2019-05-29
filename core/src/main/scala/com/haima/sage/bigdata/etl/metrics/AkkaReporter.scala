//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//
package com.haima.sage.bigdata.etl.metrics

import java.text.DateFormat
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Locale, TimeZone}

import akka.actor.ActorRef
import com.codahale.metrics._
import org.slf4j.{Logger, LoggerFactory}


object AkkaReporter {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[Builder])

  def forRegistry(registry: MetricRegistry, actor: ActorRef): AkkaReporter.Builder = {
    logger.info(s"metric path:${actor.path}")
    new AkkaReporter.Builder(registry, actor)
  }

  class Builder(registry: MetricRegistry, actor: ActorRef) {
    private var locale: Locale = Locale.getDefault
    private var clock: Clock = Clock.defaultClock
    private var timeZone: TimeZone = TimeZone.getDefault
    private var rateUnit: TimeUnit = TimeUnit.SECONDS
    private var durationUnit: TimeUnit = TimeUnit.MILLISECONDS
    private var filter: MetricFilter = MetricFilter.ALL

    def formattedFor(locale: Locale): AkkaReporter.Builder = {
      this.locale = locale
      this
    }

    def withClock(clock: Clock): AkkaReporter.Builder = {
      this.clock = clock
      this
    }

    def formattedFor(timeZone: TimeZone): AkkaReporter.Builder = {
      this.timeZone = timeZone
      this
    }

    def convertRatesTo(rateUnit: TimeUnit): AkkaReporter.Builder = {
      this.rateUnit = rateUnit
      this
    }

    def convertDurationsTo(durationUnit: TimeUnit): AkkaReporter.Builder = {
      this.durationUnit = durationUnit
      this
    }

    def filter(filter: MetricFilter): AkkaReporter.Builder = {
      this.filter = filter
      this
    }

    def build: AkkaReporter = {
      new AkkaReporter(this.registry, actor, this.locale, this.clock, this.timeZone, this.rateUnit, this.durationUnit, this.filter)
    }
  }

}

class AkkaReporter(registry: MetricRegistry, output: ActorRef, locale: Locale, clock: Clock, timeZone: TimeZone, rateUnit: TimeUnit, durationUnit: TimeUnit, filter: MetricFilter)
  extends ScheduledReporter(registry, "akka-reporter", filter, rateUnit, durationUnit) {
  private final val dateFormat: DateFormat = DateFormat.getDateTimeInstance(3, 2, locale)
  dateFormat.setTimeZone(timeZone)

  def report(gauges: util.SortedMap[String, Gauge[_]],
             counters: util.SortedMap[String, Counter],
             histograms: util.SortedMap[String, Histogram],
             meters: util.SortedMap[String, Meter],
             timers: util.SortedMap[String, Timer]) {
    if (!gauges.isEmpty) {
      output ! toJSON(gauges)(toJSON)

    }
    if (!counters.isEmpty) {
      output ! toJSON[Counter](counters)(toJSON)
    }
    if (!histograms.isEmpty) {
      output ! toJSON[Histogram](histograms)(toJSON)
    }
    if (!meters.isEmpty) {
      output ! toJSON[Meter](meters)(toJSON)

    }
    if (!timers.isEmpty) {
      output ! toJSON[Timer](timers)(printTimer)

    }
  }


  def toJSON(meter: Meter): Map[String, Any] = {
    meter.toMap
  }

  def toJSON(entry: Counter): Map[String, Any] = {
    Map("count" -> entry.getCount)
  }

  def toJSON(entry: Gauge[_]): Map[String, Any] = {
    Map("value" -> entry.getValue)
  }

  def toJSON(histogram: Histogram): Map[String, Any] = {
    histogram.getSnapshot.toMap + ("count" -> histogram.getCount)
  }


  private def toJSON[T](data: util.SortedMap[String, T])(func: (T) => Map[String, Any]): Map[String, Any] = {
    import scala.collection.JavaConversions._

    /*val name = data.head._2.getClass.getSimpleName
    Map(name + "s" -> data.map(tuple => {
      (tuple._1, func(tuple._2))
    }).toMap)*/
    data.map(tuple => {
      (tuple._1, func(tuple._2))
    }).toMap
  }

  private implicit def toMap(snapshot: Snapshot): Map[String, Any] = {
    Map("min" -> snapshot.getMin,
      "max" -> snapshot.getMax,
      "mean" -> snapshot.getMean,
      "stddev" -> snapshot.getStdDev,
      "median" -> snapshot.getMedian,
      "75%" -> snapshot.get75thPercentile,
      "95%" -> snapshot.get95thPercentile,
      "98%" -> snapshot.get98thPercentile,
      "99%" -> snapshot.get99thPercentile,
      "99.9%" -> snapshot.get999thPercentile
    )
  }

  private implicit def toMap(meter: Metered): Map[String, Any] = {
    Map("count" -> meter.getCount,
      "mean rate" -> (convertRate(meter.getMeanRate), this.getRateUnit),
      "1-minute rate" -> (convertRate(meter.getOneMinuteRate), this.getRateUnit),
      "5-minute rate" -> (convertRate(meter.getFiveMinuteRate), this.getRateUnit),
      "15-minute rate" -> (convertRate(meter.getFifteenMinuteRate), this.getRateUnit)
    )
  }

  private def printTimer(timer: Timer): Map[String, Any] = {
    timer.getSnapshot.toMap ++ timer.toMap
  }
}
