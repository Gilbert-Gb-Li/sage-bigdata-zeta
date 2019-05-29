package com.haima.sage.bigdata.analyzer.streaming.preprocessing

import java.util.Date

import com.haima.sage.bigdata.analyzer.ml.{HashingTF, IDFModel}
import com.haima.sage.bigdata.etl.common.model._
import org.apache.flink.ml.math._

class StreamVectorizationAnalyzer(override val conf: VectorizationAnalyzer) extends SimpleDataStreamAnalyzer[VectorizationAnalyzer, Any] {

  import com.haima.sage.bigdata.analyzer.preprocessing.utils.ModelHelper._
  import com.haima.sage.bigdata.analyzer.utils.preprocessing.VectorUtils._

  lazy val cfunc: (Iterable[Map[String, Any]]) => Any = conf.vectorization match {
    case _: SameWeightVectorization =>
      model: Iterable[Map[String, Any]] => {
        toSWModel(model.head)
      }
    case _: TFIDFVectorization =>
      model: Iterable[Map[String, Any]] => {
        toTFIDFModel(model.head)
      }
  }
  lazy val dfunc: (RichMap, Any) => RichMap = conf.vectorization match {
    //字段以simhash向量化
    case vectorizer: SimHashVectorization =>
      (d: RichMap, m: Any) => {
        val vector: Vector = d.get(conf.field) match {
          case Some(x: Array[String]) =>
            DenseVector(vectorizer.simHash(x))
          case _ =>
            DenseVector(vectorizer.simHash(Array()))
        }
        add(d, vector)
      }

    //        //字段以word2vec向量化
    //        case "word2vec"=>

    case vectorized: SameWeightVectorization if vectorized.isDate =>
      (d: RichMap, m: Any) => {
        d.get(conf.field) match {
          case Some(date: Date) if date != null =>
            add(d, date2Vector(date: Date))
          case _ =>
            d

        }

      }
    case vectorized: SameWeightVectorization =>


      (d: RichMap, m: Any) => {
        val models = m.asInstanceOf[Map[String, SparseVector]]
        d.get(conf.field) match {
          case Some(v) if v != null =>
            add(d, models(v.toString))
          case _ =>
            add(d, models(""))

        }

      }

    //字段以tf-idf向量化

    case _: TFIDFVectorization =>
      val tf = new HashingTF()
      (d: RichMap, m: Any) => {
        val model = m.asInstanceOf[IDFModel]
        val data = d.get(conf.field).map {
          case d: Array[String] =>
            tf.transform(d.toList)
          case _ =>
            d.get(conf.field + "_terms").map {
              case v: Array[String] =>
                tf.transform(v.toList)
            }.getOrElse(tf.empty)
        }.getOrElse(tf.empty)
        add(d, model.transform(data))
      }


  }


  override def convert(model: Iterable[Map[String, Any]]): Any = {
    cfunc(model)

  }

  override def analyzing(in: RichMap, resultFuture: Any): RichMap = {
    dfunc(in, resultFuture)
  }
}
