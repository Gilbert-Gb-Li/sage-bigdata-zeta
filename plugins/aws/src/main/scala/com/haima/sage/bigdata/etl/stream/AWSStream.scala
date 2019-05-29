package com.haima.sage.bigdata.etl.stream

import java.io.IOException
import java.util.concurrent.{TimeUnit, TimeoutException}
import java.util.{Calendar, Date}

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.regions.Region
import com.amazonaws.services.cloudtrail.model.LookupEventsRequest
import com.amazonaws.services.cloudtrail.{AWSCloudTrail, AWSCloudTrailAsyncClient}
import com.amazonaws.services.cloudwatch.model.{GetMetricStatisticsRequest, Metric}
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClient}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.{PROCESS_WAIT_TIMES, PROCESS_WAIT_TIMES_DEFAULT}
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model.Stream

import scala.util.Try

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object AWSStream {
  def apply(source: AWSCredentials, server: String, region: Region, start: Date) = {
    server match {
      case "trail" =>
        new TrailStream(source, region, start)
      case "watch" =>
        new WatchStream(source, region, start)
    }

  }


  class TrailStream(credentials: AWSCredentials, region: Region, start: Date) extends Stream[RichMap](None) {
    val WAIT_TIME: Long = Try(Constants.CONF.getLong(PROCESS_WAIT_TIMES)).getOrElse(PROCESS_WAIT_TIMES_DEFAULT)

    import scala.collection.JavaConversions._

    val cloudTrail: AWSCloudTrail = new AWSCloudTrailAsyncClient(credentials)
    cloudTrail.setRegion(region)
    var request = new LookupEventsRequest()
    request.setStartTime(start)
    request.setMaxResults(1000)
    var rt = cloudTrail.lookupEvents(request)
    var events = rt.getEvents.iterator()


    override def hasNext: Boolean = {
      state match {
        case State.init =>
          if (events.hasNext) {
            ready()
            true
          } else {
            request.setNextToken(rt.getNextToken)
            rt = cloudTrail.lookupEvents(request)
            if (rt.getEvents.isEmpty) {
              TimeUnit.MILLISECONDS.sleep(WAIT_TIME)
              request.setNextToken(rt.getNextToken)
              rt = cloudTrail.lookupEvents(request)

              if (rt.getEvents.isEmpty) {
                throw new TimeoutException(s"after $WAIT_TIME aws timeout ")
              }
            }
            events = rt.getEvents.iterator()
            hasNext
          }

        case State.ready =>
          true
        case _ =>
          false
      }
    }


    @throws(classOf[IOException])
    override def close() {
      super.close()
      cloudTrail.shutdown()
    }

    override def next(): RichMap = {

      val event = events.next()
      Map(
        "CloudTrail" -> event.getCloudTrailEvent,
        "id" -> event.getEventId,
        "name" -> event.getEventName, "@timestamp" -> event.getEventTime,
        "user_name" -> event.getUsername,
        "resources" ->
          event.getResources.map(rs => Map(
            "name" -> rs.getResourceName,
            "type" -> rs.getResourceType)).toArray

      )
    }
  }

  class WatchStream(credentials: AWSCredentials, region: Region, start: Date) extends Stream[RichMap](None) {


    var current = {
      val calendar = Calendar.getInstance()

      if (calendar.getTime.before(start)) {
        calendar.setTime(start)
      }
      calendar
    }

    def nextDate = {

      val calendar = current
      calendar.add(Calendar.DAY_OF_MONTH, 1)
      val n = Calendar.getInstance()

      if (calendar.after(n)) {
        n
      } else {
        calendar
      }
    }

    var end = nextDate

    import scala.collection.JavaConversions._

    val watch: AmazonCloudWatch = new AmazonCloudWatchClient(credentials)
    watch.setRegion(region)

    private def metrics(watch: AmazonCloudWatch) = {
      watch.listMetrics().
        getMetrics.toList
    }


    def dataPoints() = {
      metric = nextMetric()
      if (metric == null) {
        null
      } else {
        val request = new GetMetricStatisticsRequest()

        request.setStartTime(current.getTime)
        request.setEndTime(end.getTime)
        request.setNamespace(metric.getNamespace)
        request.setMetricName(metric.getMetricName)
        request.setDimensions(metric.getDimensions)
        request.setPeriod(60)
        import scala.collection.JavaConversions._
        request.setStatistics(List("Sum", "Average", "SampleCount", "Maximum", "Minimum"))
        val statistics = watch.getMetricStatistics(request)
        statistics.getDatapoints.iterator
      }

    }

    var metricss = metrics(watch)
    var metric: Metric = null

    def nextMetric(): Metric = {
      metricss match {
        case head :: tails =>
          metricss = tails
          head
        case Nil =>
          current = end
          end = nextDate

          if (current.get(Calendar.DAY_OF_MONTH) != end.get(Calendar.DAY_OF_MONTH)) {
            metricss = metrics(watch)
          } else {
            TimeUnit.HOURS.sleep(1)
          }
          nextMetric()
      }
    }


    var events = dataPoints()

    override def hasNext: Boolean = {
      state match {
        case State.init =>
          if (events.hasNext) {
            ready()
            true
          } else {
            events = dataPoints()
            if (events == null) {
              false
            } else {
              hasNext
            }
          }

        case State.ready =>
          true
        case _ =>
          false
      }
    }

    override def next(): RichMap = {

      val event = events.next()
      Map(
        "Average" -> event.getAverage,
        "Maximum" -> event.getMaximum,
        "SampleCount" -> event.getSampleCount, "@timestamp" -> event.getTimestamp,
        "Sum" -> event.getSum,
        "Unit" -> event.getUnit,
        "metric" -> Map("Namespace" -> metric.getNamespace, "name" -> metric.getMetricName)
      )
    }

    @throws(classOf[IOException])
    override def close() {
      super.close()
      watch.shutdown()
    }

  }


}


