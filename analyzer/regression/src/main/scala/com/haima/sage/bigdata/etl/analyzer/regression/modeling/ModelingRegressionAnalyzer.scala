package com.haima.sage.bigdata.analyzer.regression.modeling


import com.haima.sage.bigdata.analyzer.ml.model.Instance
import com.haima.sage.bigdata.analyzer.ml.optimization.WeightedLeastSquares
import com.haima.sage.bigdata.analyzer.regression.model.PolynomialCalculate
import com.haima.sage.bigdata.analyzer.regression.util.DataUtil._
import org.apache.flink.api.scala.utils._
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, RegressionAnalyzer, RichMap}
import com.haima.sage.bigdata.analyzer.modeling.DataModelingAnalyzer
import com.haima.sage.bigdata.analyzer.regression.util.DataUtil._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.{LabeledVector, ParameterMap, WeightVector}
import org.apache.flink.ml.math.{Breeze, _}
import org.apache.flink.ml.preprocessing.{PolynomialFeatures, Splitter}
import org.apache.flink.ml.regression.MultipleLinearRegression

class ModelingRegressionAnalyzer(override val conf: RegressionAnalyzer,
                                 override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER) extends DataModelingAnalyzer[RegressionAnalyzer](conf) with Serializable {

  //lazy protected val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


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
    getModel(conf.useModel) match {
      case Some(model) =>
        Some(env.fromElements(model))
      case None =>
        None
    }
  }

  /**
    * 训练模型
    *
    * @param data
    * @return
    */
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = {
    //拆分数据集
    val trainTestData= Splitter.trainTestSplit(data,0.6,false)
    val trainingData = trainTestData.training
    val testingData = trainTestData.testing
    val resultmodel= train(trainingData).map(weightVector => {
      weightVector.weights match {
        case SparseVector(size, indices, d) =>
          Map("intercept" -> weightVector.intercept,
            "type" -> "sparse",
            "size" -> size,
            "indices" -> indices.mkString(","),
            "data" -> d.mkString(","))
        case DenseVector(d) =>
          Map("intercept" -> weightVector.intercept,
            "type" -> "dense",
            "data" -> d.mkString(","))
      }
    })
    val resultModel=resultmodel.map(RichMap(_))
//    val resmodel=resultModel.collect()
//    println("这是模型参数",resmodel)
    val predictRe =actionWithModel(testingData,Option(resultModel))
//    val predictresult= predictRe.collect()
//    println("这是预测数据跑出来的结果",predictresult)
    //结果进行抽样（因为结果可能会太多 进行抽样）
//    val predictsample = predictRe.sample(false,0.5)
    val predictsample = testingData.sample(false,0.5,2000)
    println("predictsample-count",predictsample)
    val ans = predictsample.map(v=>{
      val mapper = new Mapper() {}.mapper
      Map("data" -> mapper.writeValueAsString(v))
    }).map(RichMap(_))
    resultModel.union(ans)
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
        val polynomialBase = PolynomialFeatures()
        polynomialBase.setDegree(conf.degree)
          val hfilter =m.filter(item => {
          val t = item.get("type")
          t.isDefined && t.exists(_ != null)
        })
        data.map(predict).withBroadcastSet(hfilter.map(t => toModel(t).get), "model")
//        data.map(predict).withBroadcastSet(m.map(t => toModel(t).get), "model")
      case None =>
        throw new UninitializedError()
    }
  }

  def train(data: DataSet[RichMap]): DataSet[WeightVector] = {


    val training = data.map(d => {
      val labeledVector = LabeledVector(d.getOrElse(conf.label, "0.0").toString.toDouble,
        d.get(conf.features).map(_.asInstanceOf[Vector]).getOrElse(DenseVector()))
      labeledVector
    }
    )

    val optimizer = new WeightedLeastSquares(true, 0.001, elasticNetParam = 0.0,
      standardizeFeatures = true, standardizeLabel = true)
    try {
      optimizer.fit(training.map(labeled => Instance(labeled.label, 1, labeled.vector))).map(rt =>
        WeightVector(rt.coefficients, rt.intercept)
      )

    } catch {
      case e: Exception =>
        e.printStackTrace()
        val polynomialBase = PolynomialFeatures()
        val mlr = MultipleLinearRegression()

        val pipeline = polynomialBase.chainPredictor(mlr)


        val parameters = ParameterMap()
          .add(PolynomialFeatures.Degree, conf.degree)
          .add(MultipleLinearRegression.Stepsize, conf.stepSize)
          .add(MultipleLinearRegression.Iterations, conf.iterations)
          .add(MultipleLinearRegression.ConvergenceThreshold, conf.convergenceThreshold)

        pipeline.fit(training, parameters)
        pipeline.predictor.weightsOption.get
    }


  }


  val predict: RichMapFunction[RichMap, RichMap] = new RichMapFunction[RichMap, RichMap] {
    var model: WeightVector = _


    override def open(parameters: Configuration): Unit = {
      import scala.collection.JavaConversions._
//      logger.warn(s"getRuntimeContext！！！:$getRuntimeContext.getBroadcastVariable[WeightVector]")
      model = getRuntimeContext.getBroadcastVariable[WeightVector]("model").head
    }

    def predict(value: Vector, model: WeightVector): Double = {
      import Breeze._
      val WeightVector(weights, weight0) = model


      val dotProduct = value.asBreeze.dot(weights.asBreeze)
      dotProduct + weight0
    }

    override def map(in: RichMap): RichMap = {
      in.get(conf.features) match {
        case Some(d: Vector) =>
          in + (conf.label -> predict(PolynomialCalculate.calculatePolynomial[Vector](conf.degree, d), model))
          //带真实值
//          in + (conf.label +"_forecast"-> predict(PolynomialCalculate.calculatePolynomial[Vector](conf.degree, d), model))
        case d =>
          logger.warn(s"data is not vector type,class is ${d.getClass}")
          in
      }
    }
  }

}
