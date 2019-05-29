package com.haima.sage.bigdata.analyzer.aggregation.modeling

import com.haima.sage.bigdata.analyzer.aggregation.model
import com.haima.sage.bigdata.analyzer.aggregation.model._
import com.haima.sage.bigdata.analyzer.aggregation.util.DataUtil._
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap


import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, LogReduceAnalyzer}
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.DataModelingAnalyzer
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
  * Created by wxn on 17-11-10.
  */
class ModelingLogReducerAnalyzer(override val conf: LogReduceAnalyzer, override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER) extends DataModelingAnalyzer[LogReduceAnalyzer](conf) {


  /*
  * 聚合数据
  *
  * */
  /*override def action(data: DataSet[RichMap]): DataSet[RichMap] = {
    if (isModel) {
      createModels(data)
    } else {
      val models: List[(Pattern, String)] = {
        if (isModel) {
          List()
        } else {
          new DataUtil().getModel(conf.useModel)
        }
      }
      data.map(d => {
        print("========================")
        d.get(conf.field.get) match {
          case Some(v) =>

            models.find(m => {
              m._1.isMatch(Terms(v.toString.trim))
            }) match {
              case Some((_, key)) =>
                d.+("isMatch" -> true, "patternKey" -> key)
              case _ =>
                println("---------------------------------")
                d.+("isMatch" -> false)
            }
          case _ =>
            print("++++++++++++++++++++++++++++++++++++")
            d.+("isMatch" -> false)
        }
      })
    }
  }*/


  /**
    * 生产数据模型
    * * @param data
    */
  def createModels(data: DataSet[RichMap]): DataSet[RichMap] = {
    data.map(d => d.getOrElse(conf.field.get, "").toString.trim)
      .filter(_ != "")
      .map(value => List(Terms(value)))
      .reduce(_ ++ _)
      .map(list => {
        val (patterns, data) = PatternUtil.getPattern(conf.minMatched, list)
        (patterns.map(_.total).sum, patterns, data)
      })
      .flatMap(patterns => {
        patterns._2.map(pattern =>
          richMap(Map(
            "cluster_total" -> patterns._1,
            "cluster_scale" -> (patterns._1 / 300).toDouble.formatted("%.2f"),
            "pattern" -> pattern.toString,
            "pattern_key" -> pattern.toKey,
            "pattern_size" -> pattern.total,
            "lcs" -> pattern.getLcs,
            "pattern_scale" -> (pattern.total / patterns._1).toDouble.formatted("%.2f"))

          ))
      })

    //    data.map(d => d.getOrElse(conf.field.get, "").toString.trim)
    //      .filter(_ != "")
    //      .map(value => List((Random.nextInt(), Terms(value)))).
    //      reduce(_ ++ _).
    //      map(d => {
    //        LogReducer(conf.minMatched).cluster(d)
    //      })
    //      .flatMap(_.toIterator)
    //      .map(c => {
    //        val (patterns, data) = c.getPattern(conf.minMatched)
    //        (c.id, patterns.map(_.total).sum, patterns, data)
    //      }).
    //      flatMap(patterns => {
    //        patterns._3.map(pattern =>
    //          Map(
    //            "cluster" -> patterns._1,
    //            "cluster_total" -> patterns._2,
    //            "cluster_scale" -> (patterns._2 / 300).toDouble.formatted("%.2f"),
    //            "pattern" -> pattern.toString,
    //            "pattern_key" -> pattern.toKey,
    //            "pattern_size" -> pattern.total,
    //            "pattern_scale" -> (pattern.total / patterns._2).toDouble.formatted("%.2f"))
    //
    //        )
    //      }) //.withBroadcastSet(dataSize, "dataSize")

    /*    data.map(d => d.getOrElse(conf.field.get, "").toString.trim)
          .filter(_ != "")
          .map(richMap).reduce(logReducer.reduce)
          .flatMap(_.toIterator)
          .map(c => {
            val (patterns, data) = c.getPattern(conf.minMatched)
            (c.id, patterns.map(_.total).sum, patterns, data)
          }).flatMap(richFlatMap).withBroadcastSet(dataSize, "dataSize")*/
    //    data.map(d => d.getOrElse(conf.field.get, "").toString.trim)
    //      .filter(_ != "")
    //      .map(richMap).reduce(reduce)
    //      .flatMap(_.toIterator)
    //      .map(c => {
    //        val (patterns, data) = c.getPattern(conf.minMatched)
    //        (c.id, patterns.map(_.total).sum, patterns, data)
    //      }).flatMap(richFlatMap).withBroadcastSet(dataSize, "dataSize")
  }

  /*合并两个聚类,如果边际密度合适*/
  def reduce: (Set[Cluster], Set[Cluster]) => Set[Cluster] = {
    (clusters, c) =>
      val data1 = c.map(cluster => {
        clusters.find(cluster.similar) match {
          case Some(temp) =>
            cluster.merge(temp)
          case _ =>
            cluster

        }
      })

      val data = data1 ++ clusters
      data


  }

  def richFlatMap: RichFlatMapFunction[(Int, Int, List[Pattern], String), Map[String, Any]] = new RichFlatMapFunction[(Int, Int, List[Pattern], String), Map[String, Any]] {

    var size: Int = _

    override def open(parameters: Configuration): Unit = {
      size = getRuntimeContext.getBroadcastVariable[Int]("dataSize").get(0)
    }

    override def flatMap(patterns: (Int, Int, List[Pattern], String), out: Collector[Map[String, Any]]): Unit = {
      patterns._3.foreach(pattern =>
        out.collect(Map(
          "cluster" -> patterns._1,
          "cluster_total" -> patterns._2,
          "cluster_scale" -> (patterns._2 / size).toDouble.formatted("%.2f"),
          "pattern" -> pattern.toString,
          "pattern_key" -> pattern.toKey,
          "pattern_size" -> pattern.total,
          "pattern_scale" -> (pattern.total / patterns._2).toDouble.formatted("%.2f")))

      )

    }
  }

  def doRichMap(): RichMapFunction[String, Set[Cluster]] = new RichMapFunction[String, Set[Cluster]] {

    import org.apache.flink.api.common.accumulators.IntCounter

    private val id = new IntCounter(0)

    override def open(parameters: Configuration): Unit = {
      getRuntimeContext.addAccumulator("num-lines", this.id)
    }

    override def map(value: String): Set[Cluster] = {
      this.id.add(1)
      val set = Set(model.Cluster(id.getLocalValuePrimitive, elements = List((id.getLocalValuePrimitive, Terms(value)))))
      set
    }
  }

  def richMap2: RichMapFunction[String, List[(Int, Terms)]] = new RichMapFunction[String, List[(Int, Terms)]] {

    import org.apache.flink.api.common.accumulators.IntCounter

    private val id = new IntCounter(0)

    override def open(parameters: Configuration): Unit = {
      getRuntimeContext.addAccumulator("num-lines", this.id)
    }

    override def map(value: String): List[(Int, Terms)] = {
      this.id.add(1)
      val set = List((id.getLocalValuePrimitive, Terms(value)))
      set
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
    val data=getModel(conf.useModel)
//    #986日志归并错误
    if (data.isEmpty){
      None
    }
    else{
    Some(env.fromCollection(data))}
  }

  /**
    * 训练模型
    *
    * @param data
    * @return
    */
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = createModels(data)

  /**
    * 执行分析
    *
    * @param data
    * @param model
    * @return
    */
  override def actionWithModel(data: DataSet[RichMap], model: Option[DataSet[RichMap]]): DataSet[RichMap] = {
    model match {
      case Some(m) =>
//        val da=data.collect()
        data.map(logReducerMap).withBroadcastSet(m.map(t => toLogReducerModel(t)), "model")
      case None =>
        throw new UninitializedError()
    }
  }

  def logReducerMap: RichMapFunction[RichMap, RichMap] = new RichMapFunction[RichMap, RichMap] {

    import scala.collection.JavaConversions._

    private var model: java.util.List[(Pattern, String)] = _

    override def open(parameters: Configuration): Unit = {

      model = getRuntimeContext.getBroadcastVariable[(Pattern, String)]("model")

    }

    override def map(value: RichMap): RichMap = {
      value.get(conf.field.get) match {
        case Some(v) =>
          model.find(m => {
            m._1.isMatch(Terms(v.toString.trim))
          }) match {
            case Some((_, key)) =>
              value.+("isMatch" -> true, "patternKey" -> key)
            case _ =>
              value.+("isMatch" -> false)
          }
        case _ =>
          value.+("isMatch" -> false)
      }
    }
  }
}
