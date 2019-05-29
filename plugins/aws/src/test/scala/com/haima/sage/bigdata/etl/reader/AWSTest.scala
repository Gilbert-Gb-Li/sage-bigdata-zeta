package com.haima.sage.bigdata.etl.reader

import java.util.function.Consumer
import java.util.{Calendar, Date}

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.cloudtrail.model.{Event, Trail}
import com.amazonaws.services.cloudtrail.{AWSCloudTrail, AWSCloudTrailAsyncClient}
import com.amazonaws.services.cloudwatch.model._
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClient}
import org.junit.Test


/**
  * Created by zhhuiyan on 15/11/24.
  */
class AWSTest {

  import scala.collection.JavaConversions._

  val credentials: AWSCredentials = new BasicAWSCredentials("AKIAP73ALCIUYVKG6K5Q", "Yjyi1eFGPSPYWGPdyUdBZqO0mXgE0IfB/ot65Dwc")


  @Test
  def cloudWatchTest(): Unit = {
    try {
      val cloudWatch: AmazonCloudWatch = new AmazonCloudWatchClient(credentials)
      val cn_north1 = Region.getRegion(Regions.CN_NORTH_1)
      cloudWatch.setRegion(cn_north1)
      /* val describeRequest = new DescribeAlarmsRequest()
       cloudWatch.describeAlarmHistory().getAlarmHistoryItems.forEach {
         new Consumer[AlarmHistoryItem] {
           override def accept(alarmHistoryItem: AlarmHistoryItem): Unit = {
             println("a|" + alarmHistoryItem)
           }
         }
       }*/
      val calendar = Calendar.getInstance()
      calendar.set(Calendar.HOUR_OF_DAY, 0)
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.YEAR, 2015)
      calendar.set(Calendar.MONTH, 11)
      calendar.set(Calendar.DAY_OF_MONTH, 24)

      val start = calendar.getTime
      println("start---:" + start)
      calendar.set(Calendar.DAY_OF_MONTH, 27)

      val end = calendar.getTime

      println("end---:" + end)
      def dataPoints(metric: Metric, start: Date, end: Date): List[Datapoint] = {
        val request = new GetMetricStatisticsRequest()

        request.setStartTime(start)

        calendar.set(Calendar.DAY_OF_MONTH, 27)

        request.setEndTime(end)
        request.setNamespace(metric.getNamespace)
        request.setMetricName(metric.getMetricName)

        /*
        * metric:{Namespace: AWS/S3,MetricName: NumberOfObjects,Dimensions: [{Name: BucketName,Value: hslog}, {Name: StorageType,Value: AllStorageTypes}]}
        * */
        val dimensions = metric.getDimensions
        dimensions.add(new Dimension().withName("BucketName").withValue("hslog"))
        dimensions.add(new Dimension().withName("StorageType").withValue("AllStorgeTypes"))
        request.setDimensions(dimensions)
        request.setPeriod(480)
        import scala.collection.JavaConversions._
        request.setStatistics(List("Sum", "Average", "SampleCount", "Maximum", "Minimum"))
        val statistics = cloudWatch.getMetricStatistics(request)
        statistics.getDatapoints.toList
      }

      def metrics() = {
        cloudWatch.listMetrics().
          getMetrics
      }

      metrics().foreach { metric =>
        println("metric:" + metric)
        dataPoints(metric, start, end).foreach(dataPint => println("dataPint___" + dataPint))
      }


      cloudWatch.shutdown()

    } catch {
      case e: Exception =>
        e.printStackTrace()

    }
  }

  @Test
  def cloudTrailTest(): Unit = {
    try {

      //  val cloudTrail:AmazonCloudTrail =new
      val cloudTrail: AWSCloudTrail = new AWSCloudTrailAsyncClient(credentials)
      val cn_north1 = Region.getRegion(Regions.CN_NORTH_1)
      cloudTrail.setRegion(cn_north1)
      cloudTrail.describeTrails().getTrailList.forEach(new Consumer[Trail] {
        override def accept(trail: Trail): Unit = {
          println("trail|" + trail)
        }
      })
      /*  cloudTrail.listPublicKeys().getPublicKeyList.forEach(new Consumer[PublicKey] {
          override def accept(trail: PublicKey): Unit = {
            println("PublicKey|" + trail)
          }
        })*/

      cloudTrail.lookupEvents().getEvents.forEach(new Consumer[Event] {
        override def accept(trail: Event): Unit = {
          println("Event|" + trail)
          trail.getEventTime
        }
      })

      cloudTrail.shutdown()
      /* cloudWatch.listMetrics().
         getMetrics.
         forEach(new Consumer[Metric] {
           override def accept(metric: Metric): Unit = {
             println("m|" + metric)
           }
         })

       val request= new GetMetricStatisticsRequest()
       val calendar=Calendar.getInstance()
       calendar.set(Calendar.HOUR_OF_DAY,0)
       calendar.set(Calendar.MINUTE,0)
       calendar.set(Calendar.SECOND,0)
       /*calendar.add(Calendar.DAY_OF_MONTH,-1)*/
       println("start---:"+calendar.getTime)
       request.setEndTime(calendar.getTime)
       calendar.add(Calendar.DAY_OF_MONTH,-7)

       println("end---:"+calendar.getTime)
       request.setStartTime(calendar.getTime)
       request.setNamespace("AWS/S3")
       request.setMetricName("BucketSizeBytes")
       request.setPeriod(480)
       import scala.collection.JavaConversions._
       request.setStatistics(List("Sum"))
       val statistics=   cloudWatch.getMetricStatistics(request)

       val datapoints= statistics.getDatapoints
       datapoints.forEach(new Consumer[Datapoint] {
         override def accept(datapoint: Datapoint): Unit = {
           println("d|" + datapoint)
         }
       })
 */
    } catch {
      case e: Exception =>
        e.printStackTrace()

    }
  }


}
