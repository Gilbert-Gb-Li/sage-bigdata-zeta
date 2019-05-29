package com.haima.sage.bigdata.analyzer.timeseries.modeling

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.haima.sage.bigdata.analyzer.timeseries.models._
import com.haima.sage.bigdata.analyzer.timeseries.util.DataUtil
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.filter.ByKnowledge
import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, RichMap, SlidingWindow, TimeSeriesAnalyzer, TumblingWindow, ARIMA => ARIMAModelView, HoltWinters => HoltWintersModelView}
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}
import com.haima.sage.bigdata.analyzer.modeling.DataModelingAnalyzer
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.ml.math._

import scala.util.Try

/**
  * Created by evan on 17-8-29.
  */
class ModelingTimeSeriesAnalyzer(override val conf: TimeSeriesAnalyzer,
                                 override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER)
  extends DataModelingAnalyzer[TimeSeriesAnalyzer](conf) with Serializable {
  lazy val dataUtil = new DataUtil()
  lazy protected val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  val fields: Array[String] = conf.field match {
    case null =>
      Array()
    case d =>
      d.split(",")
  }
  // val model: List[(String, Map[String, Any], TimeSeriesModel)] = dataUtil.getTimeSeriesModel(conf.useModel, Some(conf.timeSeriesModel.name))

  val keys: Array[String] = conf.keys match {
    case null =>
      Array()
    case d =>
      d.split(",")
  }

  val backends: Array[String] = conf.backend match {
    case null =>
      Array()
    case d =>
      d.split(",")
  }

  /*
    * 提取时间
    * */
  def extractTimestamp(element: Any): Long = {
    element match {
      case Some(d: Int) =>
        d.toLong
      case Some(d: Long) =>
        d
      case Some(d: Date) =>
        d.getTime
      case Some(d: Double) =>
        d.toLong
      case Some(d: Float) =>
        d.toLong
      case Some(d: Short) =>
        d.toLong
      case Some(d: java.lang.String) =>
        val date = d.replace("Z", " UTC")
        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z")
        sdf.parse(date).getTime
      case d =>
        logger.warn(s"field[${conf}] is $d,class[${d.getClass}] in data[${element}]")
        0L
    }
  }

  //数据聚合
  def reduced: (Temp, Temp) => Temp = {
    (first, second) => {
      var sorted = first.data.zip(second.data).map(tuple => (tuple._1 ++ tuple._2).sortWith(_._1 < _._1))
      //var granularity= if(first._2.isDefined) first._2 else first._2.map(d => second._2.get - d)
      var granularity = if (sorted(0).length > 1) sorted(0)(1)._1 - sorted(0)(0)._1 else sorted(0)(0)._1
      val (endTime, timeGranularity, third) = if (first.time < second.time)
        (second.time, Some(granularity), sorted)
      else
        (first.time, Some(granularity), sorted)

      Temp(endTime, timeGranularity, third, first.groups, first.backend)
    }
  }


  /**
    * 算法是否可以进行“生产模型”
    *
    * @return
    */
  override def modelAble: Boolean = true

  /**
    * 加载模型
    *
    * @return
    */
  override def load()(implicit env: ExecutionEnvironment): Option[DataSet[RichMap]] = {
    (conf.useModel match {
      case Some(id: String) if id != null && id.trim != null =>
        val helper: KnowledgeUser = KnowledgeSingle(ByKnowledge(id)) //id对应就是模型库的表名
        helper.getAll().map(RichMap(_)).toList
      case _ =>
        List()
    }) match {
      case null =>
        None
      case Nil =>
        None
      case data =>
        Some(env.fromCollection(data))
    }
  }

  /**
    * 训练模型
    *
    * @param data
    * @return
    */
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = {

    //训练集
    val trainingData = data.zipWithIndex.filter(dataUtil.trainfilter)
      .withBroadcastSet(data.map(_ => 1l).reduce(_ + _), "total_count")
    //utils.DataSetUtils(trainingData).zipWithIndex
    //测试集
    val testingData = data.zipWithIndex.filter(dataUtil.testfilter)
      .withBroadcastSet(data.map(_ => 1l).reduce(_ + _), "total_count")

    val test_data = testingData.map(v => {
      val time = extractTimestamp(v._2.get(conf.timeField))
      val datas = fields.map(f => Array((time, v._2.getOrElse(f, 0).toString.toDouble))).toArray

      val window = (time / ((conf.window match {
        case Some(TumblingWindow(w, _)) =>
          w
        case Some(SlidingWindow(s, w, _)) =>
          w
        case _ =>
          1
      }) * 1000)).toInt //对数据进行切分，模仿流式处理中的时间窗口
      Temp(time, None: Option[Long], datas,
        v._2.filter(t => keys.contains(t._1)),
        Map("window" -> window)
      )
    }).groupBy(d => (d.groups ++ d.backend).values.mkString(",").hashCode).reduce(reduced)

    val ts_data = trainingData.map(_._2).map(d => {
      val time = extractTimestamp(d.get(conf.timeField))
      val datas = fields.map(f => Array((time, d.getOrElse(f, 0).toString.toDouble))).toArray
      Temp(time, None: Option[Long], datas,
        d.filter(t => keys.contains(t._1)),
        d.filter(t => backends.contains(t._1))
      )
    }).groupBy(d => d.groups.values.mkString(",").hashCode).reduce(reduced)

    val result = ts_data.filter(d => d.diff.isDefined).flatMap(d => {
      //时间粒度不能为0
      val diff = if (d.diff.get == 0) 1000 else d.diff.get
      val samplingDensity = diff / 1000
      val periods = (conf.forecast / samplingDensity).toInt
      logger.warn("conf.forecast:" + conf.forecast + "----------diff:" + diff)
      logger.warn("(conf.forecast/samplingDensity).toInt:" + periods)
      val mapper = new Mapper() {}.mapper
      fields.zip(d.data).map[RichMap, Array[RichMap]] {
        case (key, value) =>
          val models = DenseVector(value.map(_._2))
          val trainModel = conf.timeSeriesModel match {
            case m: HoltWintersModelView =>
              val period = (m.period / samplingDensity).toInt
              //模型周期转化为周期内数据个数
              HoltWinters.fitModel(models, period, "additive")
            case _: ARIMAModelView =>
              ARIMA.autoFit(models)
          }
          Map("model" -> mapper.writeValueAsString(trainModel),
            "field" -> key,
            "groups" -> mapper.writeValueAsString(d.groups))
      }.toIterator
    })

    val forecast = test_data.filter(d => d.diff.isDefined).map(mapped)
      .withBroadcastSet(result, "model")
      .flatMap(array => array.toIterator)

    //预测的结果
    val left = forecast.flatMap(v => {
      lazy val mapper = new Mapper() {}.mapper
      val time = extractTimestamp(v.get(conf.timeField)).toString
      val values = v.filter(t => keys.contains(t._1))
      fields.map(m => {
        val field = m + "_forecast"
        //（时间，预测字段名，分组字段名，预测值）
        (time, m, mapper.writeValueAsString(values), v.get(field).get.toString)
      })
    })

    //原始数据
    val right = testingData.flatMap(vd => {
      val v = vd._2
      val time = extractTimestamp(v.get(conf.timeField)).toString
      lazy val mapper = new Mapper() {}.mapper
      val values = v.filter(t => keys.contains(t._1))
      fields.map(m => {
        //（时间，预测字段名，分组字段名，原始值）
        (time, m, mapper.writeValueAsString(values), v.get(m).get.toString)
      })
    })

    // 预测结果和原始数据进行合并
    val ans = left.join(right).where(0, 1, 2).equalTo(0, 1, 2).apply {
      (left, right) => {
        //（时间，预测字段，分组字段，原始值，预测值）
        (left._1, left._2, left._3, left._4, right._4)
      }
    }.groupBy(1, 2).reduceGroup(iter => {
      val group = iter.toList
      (group.head._2 + group.head._3, group.map(d => Map(conf.timeField -> dataUtil.tranTimeToString(d._1), d._2 -> d._4, (d._2 + "_forecast") -> d._5)))
    })
    //ans.writeAsText("./target/out", FileSystem.WriteMode.OVERWRITE)
    result.join(ans).where(d => {
      d("field").toString + d("groups").toString
    }).equalTo(t => t._1).apply {
      (model, d) =>
        lazy val mapper = new Mapper() {}.mapper
        model + ("data" -> mapper.writeValueAsString(d._2))
    }
  }

  def mapped: RichMapFunction[Temp, Array[RichMap]] = new RichMapFunction[Temp, Array[RichMap]]() {
    import scala.collection.JavaConversions._
    private lazy val model: List[(String, Map[String, Any], TimeSeriesModel)] = dataUtil.getTimeSeriesModel(getRuntimeContext.getBroadcastVariable[RichMap]("model").toList, Some(conf.timeSeriesModel.name))
    override def map(d: Temp): Array[RichMap] = {

      //时间粒度不能为0
      val diff = if (d.diff.get == 0) 1000 else d.diff.get
      val samplingDensity = diff / 1000
      val periods = (conf.forecast / samplingDensity).toInt
      logger.warn("conf.forecast:" + conf.forecast + "----------diff:" + diff)
      logger.warn("(conf.forecast/samplingDensity).toInt:" + periods)
      val now = d.time
      val cal = Calendar.getInstance()
      cal.setTimeInMillis(now)
      fields.zip(d.data).map {
        case (key, value) =>
          val models = DenseVector(value.map(_._2))
          val results = model.find(tree => tree._1 == key && tree._2.equals(d.groups)) match {
            case None =>
              conf.timeSeriesModel match {
                case m: HoltWintersModelView =>
                  val period: Int = (m.period / samplingDensity).toInt
                  Try(HoltWinters.fitModel(models, period, m.modelType.toString.toLowerCase()).forecast(models, periods))
                    .getOrElse(DenseVector(Array.fill(0)(0.0)))
                case _: ARIMAModelView =>
                  ARIMA.autoFit(models).forecast(models, periods)
              }
            case Some((_, _, m)) =>
              m.forecast(models, periods)
          }
          val slice = results.valueIterator.drop(models.size)
          slice.toArray.map(d => key + "_forecast" -> d)
      }.foldLeft(Array(Map[String, Any]())) {
        case (con, data) if (con.isEmpty ||con(0).isEmpty) =>
          data.map(Map[String, Any](_))
        case (con, data) =>
          con.zip(data).map(tuple => tuple._1 + tuple._2)
      }.map {
        da =>
          cal.add(Calendar.MILLISECOND, diff.toInt)
          RichMap(da + (conf.timeField -> cal.getTime) ++ d.groups)
      }
    }
  }

  /**
    * 执行分析
    *
    * @param data
    * @param model
    * @return
    */
  override def actionWithModel(data: DataSet[RichMap], model: Option[DataSet[RichMap]]): DataSet[RichMap] = {
    val ts_data = data.map(d => {
      val time = extractTimestamp(d.get(conf.timeField))
      val datas = fields.map(f => Array((time, d.getOrElse(f, 0).toString.toDouble))).toArray
      Temp(time, None: Option[Long], datas,
        d.filter(t => keys.contains(t._1)),
        d.filter(t => backends.contains(t._1))
      )
    }).groupBy(d => d.groups.values.mkString(",").hashCode).reduce(reduced)

    ts_data.filter(d => d.diff.isDefined).map(mapped)
      .withBroadcastSet(model.get, "model")
      .flatMap(array => array.toIterator)
  }
}
