package com.haima.sage.bigdata.analyzer.preprocessing.modeling

import java.util.Date

import com.haima.sage.bigdata.analyzer.ml.{HashingTF, IDF, IDFModel}
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.DataModelingAnalyzer
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.{DenseVector, SparseVector, _}

import scala.util.Try

/**
  * Created by ASUS on 2018/2/28.
  */
class ModelingVectorizationAnalyzer(override val conf: VectorizationAnalyzer,
                                    override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER)
  extends DataModelingAnalyzer[VectorizationAnalyzer](conf) {

  import com.haima.sage.bigdata.analyzer.preprocessing.utils.ModelHelper._
  import com.haima.sage.bigdata.analyzer.utils.preprocessing.VectorUtils._


  /**
    * 算法是否可以进行“生产模型”
    *
    * @return
    */
  override def modelAble: Boolean = {
    conf.vectorization match {

      case vectorized: SameWeightVectorization if vectorized.isDate =>
        false
      case vectorizer: SimHashVectorization =>
        false
      case _: SameWeightVectorization => true
      case _: TFIDFVectorization =>
        true
    }
  }



  def simHash(data: SparseVector, size: Int): Vector = {


    DenseVector(data.indices.zip(data.data).map {
      d => {
        /*hash key*/
        val binary = d._1.toBinaryString.toCharArray

        /*补充缺少位数*/
        val array = ("0".*(size - binary.length).toCharArray ++ binary).map {
          /*增加权重*/
          case '1' =>
            d._2
          case _ =>
            -d._2
        }

        array
      }
    }.foldLeft(Array[Double]()) {
      /*求和*/
      case (c, current) =>
        if (c.isEmpty) {
          current
        } else {
          c.zip(current).map { case (a, b) => a + b }
        }
    }.map {
      /*降维*/
      case d if d > 0 =>
        1
      case _ =>
        0
    })

  }

  def sameWeightMap: RichMapFunction[RichMap, RichMap] = new RichMapFunction[RichMap, RichMap] {

    private var model: Map[String, SparseVector] = _

    override def open(parameters: Configuration): Unit = {
      model = getRuntimeContext.getBroadcastVariable[Map[String, SparseVector]]("model").get(0)

    }

    override def map(value: RichMap): RichMap = {

      value.get(conf.field) match {
        case Some(date: Date) if date != null =>
          add(value, date2Vector(date: Date))
        case Some(d) if d != null =>
          add(value, model(d.toString))
        case _ =>
          add(value, model(""))

      }
    }
  }


  def tfidfMap: RichMapFunction[RichMap, RichMap] = new RichMapFunction[RichMap, RichMap] {

    private var model: IDFModel = _
    val tf = new HashingTF()

    override def open(parameters: Configuration): Unit = {
      model = getRuntimeContext.getBroadcastVariable[IDFModel]("model").get(0)

    }

    override def map(value: RichMap): RichMap = {


      val data = value.get(conf.field).map {
        case d: Array[String] =>
          DenseVector(d.map(_.hashCode.toDouble))
        case _ =>
          value.get(conf.field + "_terms").map {
            case v: Array[String] =>
              val dd = tf.transform(v.toList)
              dd
            case _ =>
              DenseVector(Array[Double]())

          }.getOrElse(DenseVector(Array[Double]()))
      }.getOrElse(DenseVector(Array[Double]()))

      val d = model.transform(data)
      //println("model-1",model.idf,d)
      add(value, d)
    }
  }


  def buildIDF(data: DataSet[RichMap]): DataSet[IDFModel] = {
    val idf = new IDF()

    val tf = new HashingTF()
    idf.fit(data.filter(d => d.contains(conf.field)).map(da => da.get(conf.field).map {
      case d: Array[String] =>
        tf.transform(d.toList)
      case _ =>
        da.get(conf.field + "_terms").map {
          case v: Array[String] =>
            val init = tf.transform(v.toList)

            init
          case _ =>
            DenseVector(new Array[Int](0))
        }.getOrElse(DenseVector(new Array[Int](0)))
    }.getOrElse(DenseVector(new Array[Int](0)))
    ))
  }

  def buildSameWeight(data: DataSet[RichMap]): DataSet[Map[String, SparseVector]] = {

    data.map(_.getOrElse(conf.field, "").toString).distinct().map(List(_)).reduce(_ ++ _).map(list => {
      val length = list.size
      list.zipWithIndex.map(tuple => {

        (tuple._1, SparseVector(length, Array(tuple._2), Array(1)))
      }).toMap

    })

  }

  //  //加载SameWeight
  //  def getSameWeight(data: DataSet[RichMap]): DataSet[Map[String, SparseVector]] = {
  //
  //    modelUser.map(_.getAll()) match {
  //      case Some(idf) =>
  //        val model = idf.head.get("data").map(d => {
  //
  //          d.toString.split(":").map(d => {
  //
  //            val kv = d.split(",")
  //            (kv(0), SparseVector(kv(1).toInt, Array(kv(2).toInt), Array(1)))
  //          }).toMap
  //        }).orNull
  //        data.getExecutionEnvironment.fromElements(model)
  //      case _ =>
  //        buildSameWeight(data: DataSet[RichMap])
  //    }
  //
  //
  //  }
  //
  //  //加载IDF
  //  def getIDF(data: DataSet[RichMap]): DataSet[IDFModel] = {
  //
  //    modelUser.map(_.getAll()) match {
  //      case Some(idf) =>
  //        val model = idf.head.get("data").map(d => {
  //
  //          val d2 = d.toString.split(" ")
  //          val size = d2(0).toInt
  //          val indices = d2(1).split(",").map(d => {
  //            d.toInt
  //          })
  //          val data = d2(1).split(",").map(d => {
  //            d.toDouble
  //          })
  //          new IDFModel(SparseVector(size, indices, data))
  //
  //        }).orNull
  //        data.getExecutionEnvironment.fromElements(model)
  //      case _ =>
  //        buildIDF(data)
  //    }
  //
  //
  //  }
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = {
    conf.vectorization match {

      case vectorized: SameWeightVectorization if vectorized.isDate =>
        data
      case vectorizer: SimHashVectorization =>
        data
      case _: SameWeightVectorization =>
        buildSameWeight(data).map(_.map(tuple => {
          s"${tuple._1},${tuple._2.size},${tuple._2.indices(0)}"
        }).mkString(":")).map(d => Map("data" -> d.toString))

      case _: TFIDFVectorization =>
        buildIDF(data).map(d => Map("data" -> d.toString))
    }
  }

  override def actionWithModel(data: DataSet[RichMap], model:Option[ DataSet[RichMap]]=None): DataSet[RichMap] = {
    conf.vectorization match {

      case vectorized: SameWeightVectorization if vectorized.isDate =>
        data.map(d => {
          d.get(conf.field) match {
            case Some(date: Date) if date != null =>
              add(d, date2Vector(date: Date))
            case _ =>
              d

          }

        })
      case vectorizer: SimHashVectorization =>

        data.map(d => {
          d.get(conf.field) match {
            case Some(x: Array[String]) =>


              val tf = new HashingTF()
              /*tf-simhash*/
              val vector = tf.transform(x.toList)
              add(d, simHash(vector, vectorizer.length))
            case Some(x: SparseVector) =>
              /*tf-idf-simhash*/
              add(d, simHash(x, vectorizer.length))

            case _ =>
              d.get(conf.field + "_terms") match{
                case Some(x: Array[String]) =>
                  val tf = new HashingTF()
                  /*tf-simhash*/
                  val vector = tf.transform(x.toList)
                  add(d, simHash(vector, vectorizer.length))
                case _ =>
                  add(d, DenseVector(new Array[Double](vectorizer.length)))
              }
          }
        })

      case _: SameWeightVectorization =>

        model match {
          case Some(m)=>
            data.map(sameWeightMap).withBroadcastSet(m.map(t => toSWModel(t.toMap)), "model")
          case _=>
            throw new UninitializedError()
        }


      case _: TFIDFVectorization =>
         model match {
          case Some(m)=>
            data.map(tfidfMap).withBroadcastSet(m.map(t => toTFIDFModel(t.toMap)), "model")
          case _=>
            throw new UninitializedError()
        }



    }
  }

  /**
    * 加载模型
    *
    * @return
    */
  override def load()(implicit env:ExecutionEnvironment): Option[DataSet[RichMap]] = {



    conf.vectorization match {

      case vectorized: SameWeightVectorization if vectorized.isDate =>
        None
      case vectorizer: SimHashVectorization =>
       None
      case _: SameWeightVectorization =>
        Try(getSameWeight(conf.useModel)).map(env.fromElements(_)).toOption
      case _: TFIDFVectorization =>
        Try(getIDF(conf.useModel)).map(env.fromElements(_)).toOption
    }

  }
}
