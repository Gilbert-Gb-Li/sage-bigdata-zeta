package com.haima.sage.bigdata.etl.server.master

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.BroadcastServer
import com.haima.sage.bigdata.etl.store.Stores

import scala.util.parsing.json.JSON

/**
  * Created by zhhuiyan on 2015/2/2.
  */
class MetricServer extends BroadcastServer {

  private val writerNamesInfo: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
  /*private val monitorNameInfo: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
  private val metricCounterFactory = scala.collection.mutable.HashMap[String, BigDecimal]()*/

  override def receive: PartialFunction[Any, Unit] = {
    case (Opt.SYNC, (name: String, info: Map[String@unchecked, Any@unchecked])) =>
      logger.debug(s"receive metric:$info")
      sender() ! Result("200", "received metric info")
//      convertToMetricInfo(name, info)
    case msg =>
      super.receive(msg)

  }

  /*private def convertToMetricInfo(name: String, metric: Map[String@unchecked, Any@unchecked]): Boolean = {
    var flag: Boolean = false
    metric.foreach {

      case (_type: String, data: Map[String@unchecked, Any@unchecked]) =>
        data.groupBy(d => getId(d._1)).foreach {
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
            // logger.debug(s"${_type} info : " + tuple._1 + "=====" + result.toString())
            flag = Stores.metricInfoStore.set(MetricInfo(name, tuple._1, _type.toUpperCase, convert(result), null))
        }
      case _ =>
    }
  flag
  }*/

  private def convert(obj: Map[String, Any]): Map[String, String] = {
    obj.map {
      case (key, value) =>
        (key, value match {
          case (data: Double, unit: String) => s"${data.formatted("%.2f")} per $unit"
          case value: Double => s"${value.formatted("%.2f")}"
          case value: Long => s"$value"
          case value: String => s"$value"
          case o => o.toString
        })
    }
  }

  private def getId(k: String): String = {
    Stores.configStore.all().map(_.id).filter(k.contains(_)) match {
      case Nil =>
        logger.debug(s"getId : $k : 异常1")
        k

      case head :: Nil =>
        /*if (writerNamesInfo.isEmpty)
          initialWriterNamesInfo()*/
        val writerNames = for (writerName <- writerNamesInfo if k.contains(s"_${writerName}_")) yield writerName

        writerNames.size match {
          case 0 => head
          case _ => k.split(s"_${writerNames.head}_").last

        }
      case _ =>
        logger.debug(s"getId : $k : 异常2")
        k
    }
  }

  // 查询配置文件中writer类型名称信息
  private def initialWriterNamesInfo(): Unit = {
    /*Stores.apiInfoStore.getProperties() match {
      case Some(json) =>
        val data: Map[String, Map[String, String]] = JSON.parseFull(json).get.asInstanceOf[Map[String, Map[String, String]]]
        data("writer").keySet.foreach { e => writerNamesInfo.+=(e) }
        logger.info(s"Initial writerNamesInfo : $writerNamesInfo")
      case None =>
        logger.warn("No properties info found !")
    }*/
  }

}
