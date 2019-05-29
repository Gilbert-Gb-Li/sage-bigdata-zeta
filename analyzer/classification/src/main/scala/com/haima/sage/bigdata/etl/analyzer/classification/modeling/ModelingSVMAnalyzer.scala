package com.haima.sage.bigdata.analyzer.classification.modeling

import java.util

import com.haima.sage.bigdata.analyzer.classification.classify.SVMClassify
import com.haima.sage.bigdata.analyzer.classification.model.SVMModel
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, RichMap, SVMAnalyzer}
import com.haima.sage.bigdata.etl.exception.IncorrectResultSizeDataAccessException
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.DataModelingAnalyzer
import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math._

import scala.collection.mutable.ArrayBuffer


class ModelingSVMAnalyzer(override val conf: SVMAnalyzer,
                          override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER) extends DataModelingAnalyzer[SVMAnalyzer](conf) with SVMClassify with Serializable {


  /*转换数据到向量*/
  private def toLabeled(positive: Option[String] = None): RichMapFunction[RichMap, LabeledVector] = new RichMapFunction[RichMap, LabeledVector] with Serializable {

    import scala.collection.JavaConversions._

    private lazy val label: String = positive.getOrElse(getRuntimeContext.getBroadcastVariable[String]("labels").toList.head)

    override def map(d: RichMap): LabeledVector = {
      val features: Option[Vector] = d.get(conf.features).map {
        case d: Vector =>
          d
        case d: List[Any@unchecked] =>
          DenseVector(d.map(_.toString.toDouble).toArray)
        case d: java.util.List[Any@unchecked] =>
          DenseVector(d.toArray().map(_.toString.toDouble))
        case d: Array[Double@unchecked] =>
          DenseVector(d)
        case d: ArrayBuffer[Double@unchecked] =>
          DenseVector(d.toArray)
      }
      val lb = d.get(conf.trainLabel) match {
        case Some(l) if l.toString == label =>
          1.0
        case _ =>
          -1.0
      }
      LabeledVector(lb, features.getOrElse(DenseVector()))
    }
  }

  //向量转模型
  private def vectorToModel: RichMapFunction[DenseVector, RichMap] = new RichMapFunction[DenseVector, RichMap] {

    import scala.collection.JavaConversions._

    private lazy val labels = getRuntimeContext.getBroadcastVariable[String]("labels")
    private lazy val total_count: Long = getRuntimeContext.getBroadcastVariable[Long]("total_count").get(0)
    private lazy val ps = getRuntimeContext.getBroadcastVariable[Long]("positive")
    private lazy val positive = if (ps.isEmpty) 0l else ps.get(0)
    private lazy val errorSum: Double = getRuntimeContext.getBroadcastVariable[(Double, Double)]("predictionPairs").toList.map {
      case (truth, prediction) =>
        Math.abs(truth - prediction)
    }.sum
    private lazy val correctRate: String = (1 - Math.abs(errorSum / total_count)).formatted("%.3f")
    private lazy val count_positive: String = (positive.toDouble / total_count).formatted("%.3f")
    private lazy val count_negative: String = (1.toDouble - count_positive.toDouble).formatted("%.3f")
    private lazy val configInfo = Array(conf.regularization.toString, conf.stepSize, conf.localIterations, conf.iterations).mkString(",")

    override def map(w: DenseVector): RichMap = {
      if(labels.size()<2){
        throw new IncorrectResultSizeDataAccessException("train data must be last have to labels")
      }

      //存储模型相关参数
      Map("weight" -> w.data.mkString(","),
        "size" -> total_count,
        "pcount" -> count_positive,
        "ncount" -> count_negative,
        "positive" -> labels.head,
        "negative" -> labels(1),
        "accuracy" -> correctRate,
        "config" -> configInfo)
    }
  }

  private def initSVM(conf: SVMAnalyzer, parallelism: Int): SVM = {
    //创建SVM模型
    SVM().
      setBlocks(parallelism). // 设置输入数据将被分割的块数
      setIterations(conf.iterations). //外循环最大迭代次数.如何往SDCA方法应用于阻塞的数据
      setLocalIterations(conf.localIterations). //SDCA最大迭代次数.定义了从每个本地数据块中提取多少数据点来计算随机双坐标上升
      setRegularization(conf.regularization). //定义权重向量更新的初始步长.如果算法变得不稳定，则必须调整该值.
      setStepsize(conf.stepSize). //定义初始化随机数生成器的种子,种子直接控制数据点被选为SDCA方法
      setSeed(0)
  }

  private def train(data: DataSet[LabeledVector], labels: DataSet[String], svm: SVM): DataSet[RichMap] = {
    //训练模型
    svm.fit(data)
    //训练模型的权值
    val weightVector = svm.weightsOption.orNull
    //用模型预测训练数据，得到该模型的正确率
    val predictionPairs = svm.evaluate(data.map(x => (x.vector, x.label)))
    val positive = data.filter(_.label == 1.0).map(_ => 1l).reduce(_ + _)

    weightVector.map(vectorToModel)
      .withBroadcastSet(labels, "labels")
      .withBroadcastSet(predictionPairs, "predictionPairs")
      .withBroadcastSet(data.map(_ => 1l).reduce(_ + _), "total_count")
      .withBroadcastSet(positive, "positive")
  }

  /*---------二分类模型实现-------------*/
  private def binary(ds: DataSet[RichMap])(implicit svm: SVM): DataSet[RichMap] = {
    //获取分类标签
    val labels = ds.map(_.getOrElse(conf.trainLabel, "").toString).distinct()
    //将数据格式转换成svm的训练输入格式
    val trainData = ds.map(toLabeled()).withBroadcastSet(labels, "labels")
    train(trainData, labels, svm)
  }

  /*---------多分类模型实现-------------*/
  private def multiple(ds: DataSet[RichMap])(implicit svm: SVM): DataSet[RichMap] = {
    //获取分类标签，根据1vs1原则，分离标签
    /*多分类,计算两次,第一次 one vs all ,then one vs one */
    /** For example:
      * {{{
      *   val input: DataSet[(String, Int)] = ...
      *   val iterated = input.iterateWithTermination(5) { previous =>
      *     val next = previous.map { x => (x._1, x._2 + 1) }
      *     val term = next.filter { _._2 <  3 }
      *     (next, term)
      *   }
      * }}}
      * */
    //.map(m=>(m.negative+m.negative,m))
    //根据1 vs 1 原则，获取一对一标签对儿
    def toLabelTuple(ds: DataSet[RichMap], label: String): DataSet[(String, String)] = {
      //获取分类标签
      val labels = ds.map(_.getOrElse(label, "").toString).distinct()
      //根据分类标签组合一对一标签对儿
      labels.cross(labels).distinct().filter(tuple => tuple._1.hashCode >= tuple._2.hashCode)
    }

    def one(origin: DataSet[RichMap], current: (String, String)): DataSet[RichMap] = {
      val labels = origin.getExecutionEnvironment.fromCollection(List(current._1, current._2))

      val data = origin.filter(new RichFilterFunction[RichMap] {
        lazy val tuple: util.List[String] = getRuntimeContext.getBroadcastVariable[String]("labels")

        override def filter(value: RichMap): Boolean = {
          if (tuple.get(0) == tuple.get(1)) {
            true
          } else {
            val lb = value.getOrElse(conf.trainLabel, "").toString
            lb == tuple.get(0) || lb == tuple.get(1)
          }
        }
      }).withBroadcastSet(labels, "labels").map(toLabeled()).withBroadcastSet(labels, "labels")
      train(data, labels, svm)
    }

    val labels = toLabelTuple(ds, conf.trainLabel)
    labels.collect().map(tuple => {
      one(ds, tuple)
    }).reduce(_.union(_))
  }


  private def classifyMap: RichMapFunction[RichMap, RichMap] = new RichMapFunction[RichMap, RichMap] {

    import scala.collection.JavaConversions._

    private lazy val models: List[RichMap] = getRuntimeContext.getBroadcastVariable[RichMap]("model").toList
    private lazy val modelWeights = models.map(model => SVMModel(model))

    override def map(in: RichMap): RichMap = {
      classify(in, modelWeights)
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
        val helper: KnowledgeUser = KnowledgeSingle(id) //id对应就是模型库的表名
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
    data.setParallelism(CONF.getInt("flink.parallelism"))
    implicit val svm: SVM = initSVM(conf, 1)
    if (conf.multi) {
      multiple(data).map(_.toMap)
    } else {
      binary(data)
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
    data.setParallelism(CONF.getInt("flink.parallelism"))
    model match {
      case Some(m) =>
        data.map(classifyMap).withBroadcastSet(m, "model")
      case None =>
        throw new UnsupportedOperationException(s"不存在svm模型，请先训练模型，再进行分类预测！")
    }
  }
}
