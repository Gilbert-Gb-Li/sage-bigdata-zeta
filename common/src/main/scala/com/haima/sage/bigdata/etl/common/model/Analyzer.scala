package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable
import java.math.BigInteger

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.haima.sage.bigdata.etl.common.model.filter.{AnalyzerRule, Tokenizer}


object ClusterType extends Enumeration {
  type Type = Value
  val FLINK, SPARK = Value
}

/**
  * Created by zhhuiyan on 15/5/7.
  */

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[NoneAnalyzer], name = "none"),
  new Type(value = classOf[LogReduceAnalyzer], name = "logaggs"),
  new Type(value = classOf[TimeSeriesAnalyzer], name = "timeseries"),
  new Type(value = classOf[SQLAnalyzer], name = "sql"),
  new Type(value = classOf[PCAAnalyzer], name = "pca"),
  new Type(value = classOf[LDAAnalyzer], name = "lda"),
  new Type(value = classOf[SVMAnalyzer], name = "svm"),
  new Type(value = classOf[RegressionAnalyzer], name = "regression"),
  new Type(value = classOf[DBSCANAnalyzer], name = "dbscan"),
  new Type(value = classOf[WordSegmentationAnalyzer], name = "word-segmentation"),
  new Type(value = classOf[VectorizationAnalyzer], name = "vectorization"),
  new Type(value = classOf[ScalarAnalyzer], name = "scalar")

))
trait Analyzer extends Parser[AnalyzerRule] {

  def id: Option[String]

  override val name: String = getClass.getSimpleName.toLowerCase.replace("analyzer", "")

  /**
    * 使用的模型加载地址
    */

  def useModel: Option[String]

  def withModel(id: String): Analyzer

  /*
  * 保存模型时,使用
  * */
  def schema(name: String = name): Schema

  def withId(id: String): Analyzer

}

case class WordSegmentationAnalyzer(field: String,
                                    tokenizer: Tokenizer,
                                    override val useModel: Option[String] = None,
                                    override val filter: Array[AnalyzerRule] = Array(),
                                    override val metadata: Option[List[(String, String, String)]] = None,
                                    override val id: Option[String] = None
                                   ) extends Analyzer {
  override val name = "word-segmentation"

  override def schema(name: String): Schema = Schema(name, Array())

  override def withId(id: String): WordSegmentationAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

case class LogReduceAnalyzer(field: Option[String] = Some("raw"),
                             minMatched: Double = 0.7,
                             override val useModel: Option[String] = None,
                             override val filter: Array[AnalyzerRule] = Array(),
                             override val metadata: Option[List[(String, String, String)]] = None,
                             override val id: Option[String] = None) extends Analyzer {
  override val name = "logaggs"

  override def schema(name: String): Schema = Schema(name, Array(
    Field("cluster", VARCHAR(200)), //聚类ID
    Field("cluster_total", DECIMAL()), //聚类数据总数
    Field("cluster_scale", DECIMAL()), //聚类占比
    Field("pattern", FieldType.CLOB), //模型信息
    Field("pattern_key", FieldType.CLOB), //模型名称
    Field("pattern_size", DECIMAL()), //模型数据数量
    Field("pattern_scale", DECIMAL()), //模型占比
    Field("lcs", VARCHAR(2000)) //
  ))

  override def withId(id: String): LogReduceAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

case class Table(
                  parserId: String,
                  tableName: String,
                  fields: Array[(String, String)],
                  timeCharacteristic: TimeType = ProcessingTime(),
                  isSideTable: Boolean = false,
                  sideTable: SideTable = AllSideTable()
                ) extends Serializable

case class JoinConfig(
                       parserId: String,
                       joinField: String,
                       @JsonScalaEnumeration(classOf[ProcessFromType])
                       timeCharacteristic: TimeType = ProcessingTime()
                     ) extends Serializable


@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[TumblingWindow], name = "tumbling"),
  new Type(value = classOf[SlidingWindow], name = "sliding"),
  new Type(value = classOf[SessionWindow], name = "session")
))
trait Window {
  val name: String = getClass.getSimpleName.replace("Window", "").toLowerCase()

  def timeCharacteristic: TimeType
}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[ProcessingTime], name = "processing"),
  new Type(value = classOf[IngestionTime], name = "ingestion"),
  new Type(value = classOf[EventTime], name = "event")
))
trait TimeType {
  val name: String = getClass.getSimpleName.replace("Time", "").toLowerCase()
}


case class ProcessingTime() extends TimeType

case class IngestionTime() extends TimeType

case class EventTime(field: String,
                     maxOutOfOrderness: Long = 10000L //最大允许的乱序时间是10s
                    ) extends TimeType {

}

case class TumblingWindow(
                           window: Long = 1000,
                           override val timeCharacteristic: TimeType = null
                         ) extends Window

case class SessionWindow(
                          gap: Long = 1000,
                          override val timeCharacteristic: TimeType = null
                        ) extends Window

case class SlidingWindow(sliding: Long = 100,
                         window: Long = 1000,
                         override val timeCharacteristic: TimeType = null
                        ) extends Window


case class SQLAnalyzer(sql: String,
                       table: Option[Table] = None,
                       override val filter: Array[AnalyzerRule] = Array(),
                       override val metadata: Option[List[(String, String, String)]] = None,
                       override val id: Option[String] = None) extends Analyzer {
  override val name: String = "sql"
  override val useModel: Option[String] = None

  override def schema(name: String): Schema = Schema(name, Array())

  override def withId(id: String): SQLAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this
}


case class Join(first: JoinConfig,
                second: JoinConfig,
                operation: Option[String] = None,
                window: Window = null
               )

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[BOBYQAOptimizer], name = "bobyqa")
))
trait Optimizer {

}

case class BOBYQAOptimizer(param: Int) extends Optimizer {
  val name: String = this.getClass.getSimpleName.replaceAll("Optimizer", "").toLowerCase
}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[CSSEstimator], name = "css")
))
trait Estimator {
  def optimizer: Option[Optimizer]

  val name: String = this.getClass.getSimpleName.replaceAll("Estimator", "").toLowerCase
}

case class CSSEstimator(override val optimizer: Option[Optimizer]) extends Estimator {

}

case class LDAAnalyzer(dimensions: Int,
                       label: String,
                       override val useModel: Option[String] = None,
                       override val filter: Array[AnalyzerRule] = Array(),
                       override val metadata: Option[List[(String, String, String)]] = None,
                       override val id: Option[String] = None) extends Analyzer {

  override val name: String = "lda"

  override def schema(name: String): Schema = Schema(name, Array(
    Field("k", NUMERIC(10, 0)),
    Field("rows", NUMERIC(10, 0)),
    Field("cols", NUMERIC(10, 0)),
    Field("data", FieldType.CLOB)
  ))

  override def withId(id: String): LDAAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

case class PCAAnalyzer(k: Int,
                       compensationValue: String,
                       minMatched: Double = 0.7,
                       override val useModel: Option[String] = None,
                       override val filter: Array[AnalyzerRule] = Array(),
                       override val metadata: Option[List[(String, String, String)]] = None,
                       override val id: Option[String] = None) extends Analyzer {

  override val name: String = "pca"

  override def schema(name: String): Schema = Schema(name, Array(
    Field("rows", NUMERIC(10, 0)),
    Field("cols", NUMERIC(10, 0)),
    Field("data", FieldType.CLOB)
  ))

  override def withId(id: String): PCAAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[ARIMA], name = "arima"),
  new Type(value = classOf[HoltWinters], name = "holtwinters")
))
trait TimeSeries {
  val name: String
}

case class ARIMA(maxP: Int = 5,
                 maxD: Int = 2,
                 maxQ: Int = 5,
                 coefficients: Array[Double],
                 hasIntercept: Boolean = true,
                 estimator: Option[Estimator] = None) extends TimeSeries {
  override val name = "arima"
}

/*
* mponent.
    *                  Additive method is preferred when seasonal variations are roughly constant through the series,
    *                  Multiplicative
* */

object HoltWintersType extends Enumeration {
  type Type = Value
  val ADDITIVE, MULTIPLICATIVE = Value
}

class HoltWintersTypeRef extends TypeReference[HoltWintersType.type]

case class HoltWinters(period: Int, //数据周期,秒
                       alpha: Double = 0.0,
                       beta: Double = 0.0,
                       gamma: Double = 0.0,
                       @JsonScalaEnumeration(classOf[HoltWintersTypeRef])
                       modelType: HoltWintersType.Type = HoltWintersType.ADDITIVE,
                       optimizer: Option[Optimizer] = Some(BOBYQAOptimizer(7))
                      ) extends TimeSeries {
  override val name = "holtwinters"
}

/**
  *
  * @param field     计算的数据字段
  * @param timeField 基于时间字段
  * @param window    计算窗口
  * @param forecast  预测时间长度,秒
  * @param filter    结果过滤器 @see [AnalyzerRule](com.haima.sage.bigdata.etl.common.model.filter.AnalyzerRule)
  * @param metadata  元数据信息
  */
case class TimeSeriesAnalyzer(field: String = "raw", //计算的数据列
                              timeField: String = "@timestamp",
                              timeSeriesModel: TimeSeries,
                              forecast: Int = 10, //预测时间长度,秒
                              //samplingDensity: Double, //数据采样密度,秒
                              backend: String = "", //保持的数据 select backend
                              keys: String = "", //分组的依据 select count(a) group by b,c,d
                              window: Option[Window] = None, //计算的窗口
                              override val useModel: Option[String] = None,
                              override val filter: Array[AnalyzerRule] = Array(),
                              override val metadata: Option[List[(String, String, String)]] = None,
                              override val id: Option[String] = None
                             ) extends Analyzer {
  override val name: String = "timeseries"

  override def schema(name: String): Schema = new Schema(name, Array(
    Field("groups", FieldType.CLOB),
    Field("model", FieldType.CLOB), //模型参数为json
    Field("field", FieldType.CLOB),
    Field("data", FieldType.CLOB)
  ))

  override def withId(id: String): TimeSeriesAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

/**
  *
  * @param degree               阶数
  * @param stepSize             更新初始步长
  * @param iterations           迭代次数
  * @param convergenceThreshold 递归阈值
  * @param label                标签(训练时)列/预测列
  * @param features             特征向量列
  * @param useModel             模型配置
  * @param filter               过滤器
  * @param metadata             其他元信息
  */
case class RegressionAnalyzer(
                               degree: Int = 1,
                               stepSize: Double = 0.1, //权重向量更新的初始步长
                               iterations: Int = 100,
                               convergenceThreshold: Double = 0.001,
                               label: String,
                               features: String,
                               keys: String = "", //分组的依据 select count(a) group by b,c,d
                               override val useModel: Option[String] = None, //训练数据地址
                               override val filter: Array[AnalyzerRule] = Array(),
                               override val metadata: Option[List[(String, String, String)]] = None,
                               override val id: Option[String] = None) extends Analyzer {

  override def schema(name: String): Schema = Schema(name, Array(
    Field("intercept", FieldType.DOUBLE), //截距
    Field("type", VARCHAR(10)), //权重向量类型
    Field("indices", FieldType.CLOB), //权重向量索引
    Field("data", FieldType.CLOB), //权重向量数据
    Field("size", NUMERIC()) //权重向量大小
  ))

  override def withId(id: String): RegressionAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

/**
  *
  * @param Iterations           ：迭代次数
  * @param Stepsize             ：初始化步长
  * @param ConvergenceThreshold ：阈值
  */
case class LogisticRegressionAnalyzer(
                                       Iterations: Int = 10, //迭代次数
                                       Stepsize: Double = 0.1, //初始化步长
                                       ConvergenceThreshold: Double = 0.002, //取的阈值
                                       override val useModel: Option[String] = None,
                                       override val filter: Array[AnalyzerRule] = Array(),
                                       override val metadata: Option[List[(String, String, String)]] = None,
                                       override val id: Option[String] = None
                                     ) extends Analyzer {
  override val name: String = "LogisticRegression"

  override def schema(name: String): Schema = new Schema(name, Array(
    Field("features", VARCHAR(20000)), //特征值数组
    Field("pLabel", VARCHAR(200)), //正类标签名称
    Field("nLabel", VARCHAR(200)), //负类标签名称
    Field("accuracy", DECIMAL()), //模型准确度
    Field("config", VARCHAR(1024)) //算法配置参数
  ))

  override def withId(id: String): LogisticRegressionAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

/**
  *
  * @param regularization  支持向量机算法的正则化常数
  * @param stepSize        权重向量更新的初始步长
  * @param iterations      外层迭代次数
  * @param localIterations 本地迭代次数
  * @param trainLabel      标签字段名称
  * @param classifyLabel   标签字段名称
  * @param features        特征值字段名称
  */
case class SVMAnalyzer(
                        regularization: Double = 0.002, //支持向量机算法的正则化常数
                        stepSize: Double = 0.1, //权重向量更新的初始步长
                        iterations: Int = 100,
                        localIterations: Int = 100,
                        trainLabel: String,
                        classifyLabel: String,
                        features: String,
                        multi: Boolean = false, //是否是多分类
                        override val useModel: Option[String] = None, //训练数据地址
                        override val filter: Array[AnalyzerRule] = Array(),
                        override val metadata: Option[List[(String, String, String)]] = None,
                        override val id: Option[String] = None) extends Analyzer {
  override val name: String = "svm"

  override def schema(name: String): Schema = Schema(name, Array(
    Field("weight", VARCHAR(20000)), //特征值数组
    Field("positive", VARCHAR(200)), //正类标签名称
    Field("negative", VARCHAR(200)), //负类标签名称
    Field("accuracy", DECIMAL()), //模型准确度
    Field("size", DECIMAL()), //训练数据集大小
    Field("pcount", DECIMAL()), //正类数据集数量
    Field("ncount", DECIMAL()), //负类数据集数量
    Field("config", VARCHAR(1024)) //算法配置参数
  ))

  override def withId(id: String): SVMAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

/**
  *
  * @param minpts 密度阈值
  * @param eps    领域半径
  * @param fields 字段名称
  */

case class DBSCANAnalyzer(minpts: Int, //密度阈值
                          eps: Int, //领域半径
                          fields: String, //字段名称
                          override val useModel: Option[String] = None,
                          override val filter: Array[AnalyzerRule] = Array(),
                          override val metadata: Option[List[(String, String, String)]] = None,
                          override val id: Option[String] = None
                         ) extends Analyzer {
  override val name: String = "dbscan"

  override def schema(name: String): Schema = Schema(name, Array())

  override def withId(id: String): DBSCANAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[SimHashVectorization], name = "simhash"),
  new Type(value = classOf[SameWeightVectorization], name = "sameweight"),
  new Type(value = classOf[TFIDFVectorization], name = "tfidf")
))
trait Vectorization extends Serializable {
  val name: String = getClass.getSimpleName.toLowerCase.replace("vectorization", "")
}

case class SimHashVectorization(length: Int) extends Vectorization {


  lazy val mask = new BigInteger("1")

  def simHash(value: Array[String]): Array[Double] = {


    val v = new Array[Int](length)

    value.foreach(temp => {
      val t = hash(temp)
      0.until(length).foreach(i => {
        if (t.and(mask.shiftLeft(i)).signum != 0)
          v(i) += 1
        else v(i) -= 1
      })
    })

    var fingerprint = new BigInteger("0")
    0.until(length).foreach(i => {
      if (v(i) >= 0) fingerprint = fingerprint.add(mask.shiftLeft(i))
    })


    var hashStr = new BigInteger(fingerprint.toString, 10).toString(2)
    if (hashStr.length < length) {
      //hash位数不够，在前面补0
      hashStr = ("0" * (length - hashStr.length)) + hashStr

    }
    hashStr.split("").map(_.toDouble)
  }

  private def hash(token: String) = if (token == null || token.length == 0)
    new BigInteger("0")
  else {
    val sourceArray = token.toCharArray
    var x = BigInteger.valueOf(sourceArray(0).toLong << 7)
    val m = new BigInteger("1000003")
    val mask = new BigInteger("2").pow(length).subtract(new BigInteger("1"))
    for (item <- sourceArray) {
      val temp = BigInteger.valueOf(item.toLong)
      x = x.multiply(m).xor(temp).and(mask)
    }
    x = x.xor(new BigInteger(String.valueOf(token.length)))
    if (x == new BigInteger("-1")) x = new BigInteger("-2")
    x
  }
}

case class TFIDFVectorization() extends Vectorization

case class SameWeightVectorization(isDate: Boolean = false) extends Vectorization

/*
* 量化数据
* */
@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[Atan], name = "atan"),
  new Type(value = classOf[NoThing], name = "nothing")
))
trait Scalar extends Serializable {
  val name: String = getClass.getSimpleName.toLowerCase.replace("scalar", "")

  def scalar(d: Double): Double
}

/*
* Atan反正切 量化数据
* */
case class Atan() extends Scalar {
  override def scalar(d: Double): Double = {
    (math.atan(d) * 2)./(math.Pi)
  }
}


/*
* Atan反正切 量化数据
* */
case class NoThing() extends Scalar {
  override def scalar(d: Double): Double = {
    d
  }
}


case class VectorizationAnalyzer(
                                  field: String,
                                  vectorization: Vectorization,
                                  override val useModel: Option[String] = None,
                                  override val filter: Array[AnalyzerRule] = Array(),
                                  override val metadata: Option[List[(String, String, String)]] = None,
                                  override val id: Option[String] = None
                                ) extends Analyzer {
  override val name: String = "vectorization"

  override def schema(name: String): Schema = Schema(name, Array(Field("data", FieldType.CLOB)))

  override def withId(id: String): VectorizationAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}

case class ScalarAnalyzer(cols: Array[(String, Scalar)],
                          override val useModel: Option[String] = None,
                          override val filter: Array[AnalyzerRule] = Array(),
                          override val metadata: Option[List[(String, String, String)]] = None,
                          override val id: Option[String] = None
                         ) extends Analyzer {
  override val name: String = "scalar"

  override def schema(name: String): Schema = Schema(name, Array(Field("data", FieldType.CLOB)))

  override def withId(id: String): ScalarAnalyzer = this.copy(id = Option(id))

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))
}


case class NoneAnalyzer(
                         override val useModel: Option[String] = None,
                         override val filter: Array[AnalyzerRule] = Array(),
                         override val metadata: Option[List[(String, String, String)]] = None,
                         override val id: Option[String] = None
                       ) extends Analyzer {

  override val name: String = "none"

  override def withModel(id: String): Analyzer = this.copy(useModel = Option(id))

  override def schema(name: String): Schema = ???

  override def withId(id: String): Analyzer = this.copy(id = Option(id))

}

