package com.haima.sage.bigdata.etl.server.worker


import java.util.Date

import akka.actor._
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.{MetricTable, _}
import com.haima.sage.bigdata.etl.store.{MetricHistoryStore, MetricInfoStore, Stores}
import com.haima.sage.bigdata.etl.utils.TimeUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

/**
  * Created by zhhuiyan on 15/1/19.
  */
class MetricWatcher extends Actor {
  private val MAX_SIZE = CONF.getInt(Constants.METRIC_SWITCH_TABLE_MAX)
  private val MIN_SIZE = CONF.getInt(Constants.METRIC_SWITCH_TABLE_MIN)
  private val SPLIT_SIZE = 12
  private val EXTRACT_FIELD_NAME = CONF.getString(Constants.METRIC_EXTRACT_BASE_FIELD)

  // class MetricWatcher(path: String) extends Actor {

  private val infoStores = Stores.metricInfoStores

  def historyStore: MetricHistoryStore = Stores.metricHistoryStore

  private[MetricWatcher] val logger = LoggerFactory.getLogger(classOf[MetricWatcher])

  private final val historyCountCache: mutable.Map[String, Long] = mutable.Map()
  /*sendIdentifyRequest()*/


  /* def remote: ActorRef = context.actorOf(ClusterClient.props(ClusterClientSettings(context.system).withInitialContacts(masters)))

   def sendIdentifyRequest() = {
     // context.actorSelection(`path`) ! Identify(path)
     remote ! ClusterClient.Send("/user/metric", Identify("metric"), localAffinity = false)
   }*/

  /*= identifying

  import scala.concurrent.duration._

  def identifying: Receive = {
    // case ActorIdentity(`path`, Some(actor)) =>
    case ActorIdentity("metric", Some(actor)) =>
      context.watch(actor)
      context.become(active(actor))
      logger.info(s" metric server connected with master[$actor]")
    //case ActorIdentity(`path`, None) =>
    case ActorIdentity("metric", None) =>
      context.system.scheduler.scheduleOnce(15 seconds) {
        sendIdentifyRequest()
      }

    case ReceiveTimeout =>
      context.system.scheduler.scheduleOnce(15 seconds) {
        sendIdentifyRequest()
      }
  }


  def active(actor: ActorRef)*/
  def receive: Receive = {
    case START =>
      logger.info(" metric server started!")
    case msg: Map[String@unchecked, Any@unchecked] =>
//      logger.debug(s"receive metrics : $msg")
      convertToMetricInfo(msg)
    // actor ! (CONF.getString("worker.id"), msg)
    case query@MetricQuery(id, _, _, _, _) =>
      val start = System.currentTimeMillis()
      logger.debug(s"Get metrics for Channel[$id].")
      val history: Map[String, Long] = historyStore.getByConfigId(id)
        .groupBy(_.metricId)
        .map {
          data =>
            (data._1, historyCountCache.getOrElse(data._1, 0L).+(data._2.map(_.count).sum))
        }
      /*val info: List[MetricInfo] = infoStore.get(id, metricPhase, metricType, from, to)
      info.groupBy(_.metricId).map {
        data =>
          data._2(data._2.size)
      }*/

      val info: List[MetricInfo]  = queryData(query)
      logger.info(s"Metric[$id] query finished, cost ${System.currentTimeMillis() - start} ms")
      sender() ! MetricWrapper(
        info,
        if (history.isEmpty) {
          historyCountCache.filter(_._1.contains(id)).toMap
        } else {
          history
        })
    /*case Terminated(master) =>
      context.unwatch(master)
      context.unbecome()
      logger.error(s" metric Detected unreachable : [${actor.path}].")
      sendIdentifyRequest()*/
    case (Opt.FLUSH, metricId: String) =>
      logger.debug(s"Flush metrics in cache, metricId = $metricId")
      defaultStore().getByMetricId(metricId) match {
        case Some(info) =>
          historyStore.set(MetricHistory(
            info.metricId,
            info.configId,
            s"${info.metricPhase}_${info.metricType}",
            getCount(info)
          ))
        case None =>
          logger.error(s"Nothing MetricHistory needed: No Metric info found, metricId = $metricId !")
      }
      historyCountCache.-=(metricId)
    case (Opt.FLUSH, table: MetricTable.MetricTable, metricId: String) =>
      //      logger.debug(s"receive flush request, table = $table, metricId = $metricId")
      /*defaultStore().getByMetricId(metricId) match {
        case Some(info) => Stores.metricInfoStores(table).set(info)
        case None =>
          logger.error(s"Nothing MetricInfo$table needed: No Metric info found, metricId = $metricId !")
      }*/

      def flush(queryFrom: MetricTable.MetricTable, flushTo: MetricTable.MetricTable, interval: Int): Unit = {
        val to = TimeUtils.currentDate
        val from = TimeUtils.minuteBefore(to, interval)
        val metrics = infoStores(queryFrom).getByMetricId(metricId, TimeUtils.defaultFormat(from), TimeUtils.defaultFormat(to))
        if (metrics != null && metrics.nonEmpty) metricsSort(metrics).foreach(infoStores(flushTo).set)
      }

      table match {
        case MetricTable.METRIC_INFO_SECOND =>
          logger.info(s"flush nothing for $table ")
        case MetricTable.METRIC_INFO_MINUTE =>
          flush(MetricTable.METRIC_INFO_SECOND, MetricTable.METRIC_INFO_MINUTE, CONF.getDuration(Constants.METRIC_INTERVAL_MINUTE).toMinutes.toInt)
        case MetricTable.METRIC_INFO_HOUR =>
          flush(MetricTable.METRIC_INFO_MINUTE, MetricTable.METRIC_INFO_HOUR, CONF.getDuration(Constants.METRIC_INTERVAL_HOUR).toMinutes.toInt)
        case MetricTable.METRIC_INFO_DAY =>
          flush(MetricTable.METRIC_INFO_HOUR, MetricTable.METRIC_INFO_DAY, CONF.getDuration(Constants.METRIC_INTERVAL_DAY).toMinutes.toInt)
      }

    case Opt.DELETE =>
      // 获取 7 天之前的时间
      val save: Int = Try(CONF.getInt(Constants.METRIC_SAVE_DAYS)).getOrElse(1)
      val before = TimeUtils.defaultFormat(TimeUtils.minuteBefore(TimeUtils.defaultParse(TimeUtils.defaultFormat(new Date()).replaceAll("\\d{2}:\\d{2}:\\d{2}", "00:00:00")), 60 * 24 * save))
      infoStores.foreach {
        data =>
          data._2.deleteBefore(before)
      }
    case (Opt.RESET, id: String) =>

      /**
        * 重置操作只能发生在通道停止，通道停止时 historyCountCache 会清除其Metrics缓存。
        */
      val success = historyStore.deleteByConfigId(id)
      logger.info(s"Reset Metrics of Channel[$id] $success")

  }

  private implicit def convertToMetricInfo(implicit metric: Map[String@unchecked, Any@unchecked]): Boolean = {
    var flag: Boolean = false

    metric.groupBy(d => getMetricName(d._1)).foreach {
      tuple =>
        val data: List[Map[String, Any]] = tuple._2.values.toList.filter(_.isInstanceOf[Map[String@unchecked, Any@unchecked]]).map {
          case d: Map[String@unchecked, Any@unchecked] =>
            d
        }
        val result: Map[String, Any] = data.reduce[Map[String, Any]] {
          case (data1, data2) =>
            data1.zip(data2).map {
              case ((k1, v1: Double), (k2, v2: Double)) =>
                (k1, v1 + v2)
              case ((k1, v1: Long), (k2, v2: Long)) =>
                (k1, v1 + v2)
              case ((k1, v1: String), (_, v2: String)) =>
                (k1, v1.toDouble + v2.toDouble)
              case ((k1, (vv1: Double, vu1)), (k2, (vv2: Double, vu2))) =>
                (k1, (vv1 + vv2, vu1))
              case ((k1, (vv1: String, vu1)), (k2, (vv2: String, vu2))) =>
                (k1, (vv1.toDouble + vv2.toDouble, vu1))
              case ((k1, v1), (k2, v2)) =>
                logger.error(k1.getClass + ":" + v1.getClass + "," + k2.getClass + ":" + v2.getClass)
                (k1, v1)
            }
        }
        val metricNameInfo = tuple._1.split("_")
        val info = MetricInfo(
          "-",
          tuple._1, {
            val _metricNameInfo = metricNameInfo(0).split("@")
            _metricNameInfo.length match {
              case 2 => _metricNameInfo(1)
              case _ => metricNameInfo(0)
            }
          },
          MetricType.withName(metricNameInfo(2).toUpperCase),
          MetricPhase.withName(metricNameInfo(1).toUpperCase),
          convert(result),
          null)
        flag = defaultStore().set(info)
        history(info)
    }

    /* metric.foreach {

       case (_type: String, data: Map[String@unchecked, Any@unchecked]) =>
         data.groupBy(d => getMetricName(d._1)).foreach {
           tuple =>
             val data: List[Map[String, Any]] = tuple._2.values.toList.filter(_.isInstanceOf[Map[String@unchecked, Any@unchecked]]).map {
               case d: Map[String@unchecked, Any@unchecked] =>
                 d
             }
             val result: Map[String, Any] = data.reduce[Map[String, Any]] {
               case (data1, data2) =>
                 data1.zip(data2).map {
                   case ((k1, v1: Double), (k2, v2: Double)) =>
                     (k1, v1 + v2)
                   case ((k1, v1: Long), (k2, v2: Long)) =>
                     (k1, v1 + v2)
                   case ((k1, v1: String), (_, v2: String)) =>
                     (k1, v1.toDouble + v2.toDouble)
                   case ((k1, (vv1: Double, vu1)), (k2, ((vv2: Double, vu2)))) =>
                     (k1, (vv1 + vv2, vu1))
                   case ((k1, (vv1: String, vu1)), (k2, ((vv2: String, vu2)))) =>
                     (k1, (vv1.toDouble + vv2.toDouble, vu1))
                   case ((k1, v1), (k2, v2)) =>
                     logger.error(k1.getClass + ":" + v1.getClass + "," + k2.getClass + ":" + v2.getClass)
                     (k1, v1)
                 }
             }
             flag = store.set(MetricInfo("-", tuple._1, _type.toUpperCase, convert(result), null))
         }
       case _ =>
     }*/
    flag
  }

  private def convert(obj: Map[String, Any]): Map[String, Any] = {
    obj.map {
      case (key, value) =>
        (key, value match {
          case (data: Double, unit: String) => data.formatted("%.2f").toDouble
          case value: Double => value.formatted("%.2f").toDouble
          case value: Long => value
          case value: String => s"$value"
          case o => o.toString
        })
    }
  }

  private def getId(k: String): String = {
    Try(k.split("_")(1)).getOrElse(k)
  }

  private def getMetricName(k: String): String = {
    Try {
      val metricName = k.split("_")
      s"${metricName(0)}_${metricName(1)}_${metricName(2)}"
    }.getOrElse(k)
  }

  private def history(info: MetricInfo): Unit = {
    val _count: Long = getCount(info)
    historyCountCache.get(info.metricId) match {
      case Some(countCache) =>
        if (countCache < _count) {
          historyCountCache.+=(info.metricId -> _count)
          //          logger.debug(s"receive metrics info ${info.metricId}: cache history count [$countCache], receive count [${_count}]")
        } else if (countCache >= _count) {

        }
      case None =>
        historyCountCache.+=(info.metricId -> _count)
    }
  }

  private def getCount(info: MetricInfo): Long = {
    info.metric("count") match {
      case int: Int =>
        int.toLong
      case long: Long =>
        long
      case _ =>
        0L
    }
  }

  private def splitTime(id: String, fromVal: Date, toVal: Date): List[(Date, Date)] = {
    var data: List[(Date, Date)] = List((fromVal, toVal))
    while (data.size < SPLIT_SIZE) {
      data = data.flatMap(d => doSplit(d._1, d._2))
    }

    def doSplit(fromVal: Date, toVal: Date): List[(Date, Date)] = {
      val splitNum = 3
      val per = Math.floor((toVal.getTime - fromVal.getTime) / splitNum).toLong
      List.range(0, splitNum).map(
        i =>
          (new Date((fromVal.getTime + i.*(per))), new Date((fromVal.getTime + (i + 1).*(per))))
      ).filter(d => defaultStore().count(id, TimeUtils.defaultFormat(d._1), TimeUtils.defaultFormat(d._2)) > 0)
    }

    data
  }

  private def defaultStore(): MetricInfoStore = infoStores(MetricTable.METRIC_INFO_SECOND)

  private def queryData(query: MetricQuery): List[MetricInfo] = {

    var store: MetricInfoStore = infoStores(MetricTable.METRIC_INFO_MINUTE)
    /*
    val time = query.to.getTime - query.from.getTime
    if (time >= 60 * 60 * 1000 && time < 24 * 60 * 60 * 1000) {
      store = infoStores(MetricTable.METRIC_INFO_MINUTE)
    } else if (time >= 24 * 60 * 60 * 1000 && time < 7 * 24 * 60 * 60 * 1000) {
      store = infoStores(MetricTable.METRIC_INFO_HOUR)
    } else if (time >= 7 * 24 * 60 * 60 * 1000) {
      store = infoStores(MetricTable.METRIC_INFO_DAY)
    }*/
    val num = count(store, query)
    if (num > MAX_SIZE) {
      store = infoStores(MetricTable.METRIC_INFO_HOUR)
      logger.debug(s"query data from table[${MetricTable.METRIC_INFO_HOUR}]")
    }
    else if (num <= MIN_SIZE) {
      store = infoStores(MetricTable.METRIC_INFO_SECOND)
      logger.debug(s"query data from table[${MetricTable.METRIC_INFO_SECOND}]")
    } else {
      logger.debug(s"query data from table[${MetricTable.METRIC_INFO_MINUTE}]")
    }
    store.get(query.configId, query.metricPhase, query.metricType, Some(TimeUtils.defaultFormat(query.from)), Some(TimeUtils.defaultFormat(query.to)))
  }

  private def count(store: MetricInfoStore, query: MetricQuery): Int = {
    store.count(query.configId, TimeUtils.defaultFormat(query.from), TimeUtils.defaultFormat(query.to))
  }

  private def extractData(query: MetricQuery): List[MetricInfo] = {
    val count = defaultStore().count(query.configId, TimeUtils.defaultFormat(query.from), TimeUtils.defaultFormat(query.to))
    logger.info(s"metrics count: $count, extract data from ${query.from} to ${query.to}")
    val info = if (count > MAX_SIZE) {
      val times = splitTime(query.configId, query.from, query.to)
      logger.info(s"extract ${times.size} metrics ")
      times.flatMap {
        time =>
          val list = defaultStore().get(query.configId, query.metricPhase, query.metricType, Some(TimeUtils.defaultFormat(time._1)), Some(TimeUtils.defaultFormat(time._2)))
            .groupBy(_.metricId)
            .map {
              data =>
                data._2(Math.floor(data._2.size / 2).toInt)
            }.toList
          list
      }
    } else {
      defaultStore().get(query.configId, query.metricPhase, query.metricType, Some(TimeUtils.defaultFormat(query.from)), Some(TimeUtils.defaultFormat(query.to)))
    }

    info
  }

  private def metricsSort(metrics: List[MetricInfo]): List[MetricInfo] = {
    val sortMetrics: mutable.TreeSet[MetricInfo] = new mutable.TreeSet[MetricInfo]()(
      new Ordering[MetricInfo] {
        override def compare(x: MetricInfo, y: MetricInfo): Int = {
          val c1 = x.metric(EXTRACT_FIELD_NAME).asInstanceOf[Double]
          val c2 = y.metric(EXTRACT_FIELD_NAME).asInstanceOf[Double]
          if (c1 > c2) 1
          else if (c1 == c2) 0
          else -1
        }
      })
    metrics.foreach(sortMetrics.add)
    val sortArray = sortMetrics.toArray
    if (sortArray.length > 2)
      List(sortArray(0), sortArray((sortArray.length - 1) / 2), sortArray(sortArray.length - 1))
    else
      sortMetrics.toList
  }
}
