package com.haima.sage.bigdata.analyzer.streaming.preprocessing

import com.haima.sage.bigdata.analyzer.streaming.SimpleDataStreamAnalyzer
import com.haima.sage.bigdata.etl.common.model.{RichMap, ScalarAnalyzer}
import org.apache.flink.ml.math.{DenseVector, Vector, _}

class StreamScalarAnalyzer(override val conf: ScalarAnalyzer) extends SimpleDataStreamAnalyzer[ScalarAnalyzer,Any]{

  import com.haima.sage.bigdata.analyzer.utils.preprocessing.VectorUtils._


  override def convert(model: Iterable[Map[String, Any]]): Any = {
    throw new NotImplementedError("scalar not useModel~")
  }

  override def analyzing(in: RichMap, resultFuture: Any): RichMap = {

    val vector = conf.cols.map {
      case (key, value) =>
        val rt = in.get(key) match {
          case Some(x: Int) =>
            DenseVector(Array(value.scalar(x)))
          case Some(x: Long) =>
            DenseVector(Array(value.scalar(x)))

          case Some(x: Float) =>
            DenseVector(Array(value.scalar(x)))

          case Some(x: Double) =>
            DenseVector(Array(value.scalar(x)))
          case Some(x: String) if x.matches("(\\d+.?\\d*|\\d*.?\\d+)") =>
            DenseVector(Array(value.scalar(x.toDouble)))

          case Some(x: Array[Double]) =>
            DenseVector(x.map(value.scalar))
          case Some(x: Vector) =>
            x
          case _ =>
            DenseVector(Array(0d))
        }
        rt

    }.foldLeft(SparseVector(0, Array(0), Array(0d)))((c, d) => {
      mergeVector(c, d).asInstanceOf[SparseVector]
    })


    if (vector.size > 0) {
      add(in, vector)
    } else {
      in
    }





  }
}
