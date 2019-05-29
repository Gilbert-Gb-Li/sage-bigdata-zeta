package com.haima.sage.bigdata.analyzer.optimization.modeling

import com.haima.sage.bigdata.analyzer.optimization.model.{PCA, PCAModel}
import com.haima.sage.bigdata.analyzer.util.DataUtils
import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, PCAAnalyzer}
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.DenseVector
import com.haima.sage.bigdata.etl.utils.Logger
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.DataModelingAnalyzer
import com.haima.sage.bigdata.analyzer.optimization.util.DataUtil._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration


/**
  * Created by jdj on 2017/11/14.
  */
class ModelingPCAAnalyzer(override val conf: PCAAnalyzer,
                          override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER) extends DataModelingAnalyzer[PCAAnalyzer](conf) with Logger {


  private def preAction(data: DataSet[RichMap]): DataSet[DenseVector] = {
    //1.需要数据做预处理的操作
    val ds = data.map(map => {
      var maps: Map[String, Double] = Map[String, Double]()
      map.foreach(t => {
        maps += ((t._1, t._2.toString.toDouble))
      })
      maps
    })
    val dataPreprocessing = DataUtils.DataPreprocessing(ds)

    val dsVector = dataPreprocessing.map(map => {

      val it = map.values.map(x => x.toString.toDouble)
      DenseVector(it.toArray)
    })
    dsVector
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
    val knowledgeId = conf.useModel.get
    Some(env.fromElements(getPCAModel(knowledgeId)))
  }

  /**
    * 训练模型
    *
    * @param data
    * @return
    */
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = {
    data.getExecutionEnvironment.fromElements(PCA.fit(conf.k, preAction(data), conf.minMatched).toMap())
  }

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
        preAction(data).map(pcaMap).withBroadcastSet(m.map(t => toPCAModel(t)), "model")
          .map(vector => {
            Map("type" -> "PCA", "time" -> System.currentTimeMillis(), "value" -> vector)
          })
      case None =>
        throw new UninitializedError()
    }
  }

  def pcaMap: RichMapFunction[DenseVector, org.apache.flink.ml.math.Vector] = new RichMapFunction[DenseVector, org.apache.flink.ml.math.Vector] {

    private var model: PCAModel = _

    override def open(parameters: Configuration): Unit = {

      model = getRuntimeContext.getBroadcastVariable[PCAModel]("model").get(0)

    }

    override def map(in: DenseVector): org.apache.flink.ml.math.Vector = {
      model.transform(in)
    }
  }
}
