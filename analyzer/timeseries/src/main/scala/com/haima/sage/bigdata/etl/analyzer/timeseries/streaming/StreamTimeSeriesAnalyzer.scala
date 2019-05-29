package com.haima.sage.bigdata.analyzer.timeseries.streaming

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.haima.sage.bigdata.analyzer.timeseries.models.{ARIMA, HoltWinters, TimeSeriesModel}
import com.haima.sage.bigdata.analyzer.timeseries.util.DataUtil
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.{EventTime, RichMap, SlidingWindow, TimeSeriesAnalyzer, ARIMA => ARIMAModelView, HoltWinters => HoltWintersModelView}
import com.haima.sage.bigdata.analyzer.streaming.DataStreamAnalyzer
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.math._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}

import scala.util.Try

private[streaming] case class Temp(backends: Map[String, Any], /*保留的字段列表*/
                                   keys: Map[String, Any], /*分组的字段列表*/
                                   startTime: Option[Long], /*聚合的时间窗起始时间*/
                                   window: Option[Long], /*时间窗时间粒度,单位毫秒*/
                                   windowData: Array[Array[Double]] /*时间窗数据*/
                                  )

/**
  * Created by zhhuiyan on 15/4/23.
  */
class StreamTimeSeriesAnalyzer(override val conf: TimeSeriesAnalyzer) extends DataStreamAnalyzer[TimeSeriesAnalyzer, Temp, Array[Map[String, Any]], List[(String, Map[String, Any], TimeSeriesModel)]] {

  lazy val dataUtil = new DataUtil()

  val fields: Array[String] = conf.field match {
    case null =>
      Array()
    case d =>
      d.split(",")
  }

  val keys: Array[String] = conf.keys match {
    case null =>
      Array()
    case d =>
      d.split(",")
  }

  val backend: Array[String] = conf.backend match {
    case null =>
      Array()
    case d =>
      d.split(",")
  }

  def reduced: (Temp, Temp) => Temp = {
    (first, second) => {
      if (first.window.isDefined) {
        Temp(first.backends, first.keys, first.startTime, first.window, first.windowData.zip(second.windowData).map(tuple => tuple._1 ++ tuple._2))
      } else {
        Temp(first.backends, first.keys, first.startTime, first.startTime.map(d => second.startTime.get - d), first.windowData.zip(second.windowData).map(tuple => tuple._1 ++ tuple._2))
      }
    }
  }

  override def convert(model: Iterable[Map[String, Any]]): List[(String, Map[String, Any], TimeSeriesModel)] = {
    dataUtil.getTimeSeriesModel(model.toList, Some(conf.timeSeriesModel.name))
  }

  override def marshaller: TypeInformation[Array[Map[String, Any]]] = createTypeInformation[Array[Map[String, Any]]]

  override def toIntermediate(data: DataStream[RichMap]): DataStream[Temp] = {
    data.executionEnvironment.setParallelism(CONF.getInt("flink.parallelism"))
    import com.haima.sage.bigdata.analyzer.streaming.utils._

    data.executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val beforeWindow = (conf.window match {
      case Some(window:SlidingWindow) =>
        data.setParallelism(CONF.getInt("flink.parallelism")).assign(EventTime(field = conf.timeField))
      case _ =>
        data.setParallelism(CONF.getInt("flink.parallelism"))
    }).map(d => {
      val time = d.get(conf.timeField).map {
        case time: Date => time.getTime
        case time: java.lang.Long => time.toLong
        case time: Long => time
        case time: java.lang.String =>
          val date = time.replace("Z", " UTC")
          val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z")
          sdf.parse(date).getTime
      }

      /*
      * 多个计算字段
      * */
      val datas = fields.map(f => Array(d.getOrElse(f, 0).toString.toDouble)).toArray

      Temp(d.filter(tuple => backend.contains(tuple._1)),
        d.filter(tuple => keys.contains(tuple._1)),
        time, //起始时间
        None: Option[Long], //时间间隔,数据
        datas)
    }).keyBy(d => keys.map(k => d.keys.getOrElse(k, "").toString).mkString("-").hashCode)

    (conf.window match {
      case Some(window : SlidingWindow) =>
        beforeWindow.window(window.copy(timeCharacteristic = EventTime(conf.timeField)).assigner()).reduce(reduced)
      case _ =>
        beforeWindow.reduce(reduced)
    }).filter(d => {
      //logger.warn(s"data[${d.backends}] start at ${d.startTime} is grouped size less then 2 is ignore")
      d.window.isDefined && d.windowData.head.length > 2
    })
  }

  override def fromIntermediate(data: DataStream[Array[Map[String, Any]]]): DataStream[RichMap] = {
    data.flatMap(array => array.toIterator).map(d => RichMap(d))
  }

  /**
    * 算法预测逻辑实现
    *
    * @param in    输入流的一条记录
    * @param model 模型数据
    * @return RichMap
    */
  override def analyzing(in: Temp, model: List[(String, Map[String, Any], TimeSeriesModel)]): Array[Map[String, Any]] = {
    if(model ==null || model.isEmpty){
      throw new ExceptionInInitializerError("时间序列需要选择模型才能执行！")
    }
    val diff = in.window.get
    /*预测条数=预测时间长度/抽样周期*/
    val forecast: Int = (conf.forecast * 1000 / diff).toInt
    val fors = fields.zip(in.windowData).map {
      case (key, value) =>
        val models = DenseVector(value)
        val results = model.find(tree => tree._1 == key && tree._2.equals(in.keys)) match {
          case None =>
            conf.timeSeriesModel match {
              case m: HoltWintersModelView =>
                val period: Int = (m.period * 1000 / diff).toInt
                Try(HoltWinters
                  .fitModel(models, period, m.modelType.toString.toLowerCase())
                  .forecast(models, forecast))
                  .getOrElse(DenseVector(Array.fill(forecast + models.size)(0.0)))

              case _: ARIMAModelView =>
                ARIMA.autoFit(models).forecast(models, forecast)
            }
          case Some((_, _, m)) =>
            Try(m.forecast(models, forecast))
              .getOrElse(DenseVector(Array.fill(forecast + models.size)(0.0)))
        }
        val slice = results.valueIterator.drop(models.size)
        slice.toArray.map(d => if (d.isNaN) key + "_forecast" -> 0.0 else key + "_forecast" -> d)
    }

    val now = in.startTime.get + (in.windowData.head.length - 1) * diff
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(now)
    logger.warn("start:" + cal.getTime)
    fors.foldLeft(Array[Map[String, Any]]()) {
      case (con, data) if con.isEmpty =>
        data.map(Map[String, Any](_))
      case (con, data) =>
        con.zip(data).map(tuple => tuple._1 + tuple._2)
    }.map {
      da =>
        cal.add(Calendar.MILLISECOND, diff.toInt)
        in.backends ++ da + (conf.timeField -> cal.getTime) + ("forecast_properties" -> conf.field)
    }

  }
}
