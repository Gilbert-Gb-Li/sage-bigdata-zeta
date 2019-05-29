package com.haima.sage.bigdata.analyzer.ml

import com.haima.sage.bigdata.etl.common.Constants.CONF
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{DenseVector, SparseVector, Vector}
import org.scalatest.{FunSuite, Matchers}

/**
  * Created by CaoYong on 2017/10/26.
  */
class IDFSuite extends FunSuite with Matchers {

  import com.haima.sage.bigdata.analyzer.util.TestingUtils._

  private val env = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(CONF.getInt("flink.parallelism"))


  test("idf") {
    val n = 4
    val localTermFrequencies: Seq[Vector] = Seq(
      SparseVector(n, Array(1, 3), Array(1.0, 2.0)),
      DenseVector(0.0, 1.0, 2.0, 3.0),
      SparseVector(n, Array(1), Array(1.0))
    )
    val m = localTermFrequencies.size
    val termFrequencies = env.fromCollection(localTermFrequencies)
    val idf = new IDF
    val model = idf.fitModel(termFrequencies)
    val expected = DenseVector(Array(0, 3, 1, 2).map { x =>
      math.log((m + 1.0) / (x + 1.0))
    })
    assert(model.idf ~== expected absTol 1e-12)

    val assertHelper = (tfidf: Array[Vector]) => {
      assert(tfidf.length === 3)
      val tfidf0 = tfidf(0).asInstanceOf[SparseVector]
      assert(tfidf0.indices === Array(1, 3))
      assert(DenseVector(tfidf0.data) ~==
        DenseVector(1.0 * expected(1), 2.0 * expected(3)) absTol 1e-12)
      val tfidf1 = tfidf(1).asInstanceOf[DenseVector]
      assert(DenseVector(tfidf1.data) ~==
        DenseVector(0.0, 1.0 * expected(1), 2.0 * expected(2), 3.0 * expected(3)) absTol 1e-12)
      val tfidf2 = tfidf(2).asInstanceOf[SparseVector]
      assert(tfidf2.indices === Array(1))
      assert(tfidf2.data(0) ~== (1.0 * expected(1)) absTol 1e-12)
    }
    // Transforms a RDD
    val tfidf = model.transform(termFrequencies).collect()
    assertHelper(tfidf.toArray)
    // Transforms local vectors
    val localTfidf = localTermFrequencies.map(model.transform).toArray
    assertHelper(localTfidf)
  }

  test("idf minimum document frequency filtering") {
    val n = 4
    val localTermFrequencies: Seq[Vector] = Seq(
      SparseVector(n, Array(1, 3), Array(1.0, 2.0)),
      DenseVector(0.0, 1.0, 2.0, 3.0),
      SparseVector(n, Array(1), Array(1.0))
    )
    val m = localTermFrequencies.size
    val termFrequencies = env.fromCollection(localTermFrequencies)
    val idf = new IDF(minDocFreq = 1)
    val model = idf.fitModel(termFrequencies)
    val expected = DenseVector(Array(0, 3, 1, 2).map { x =>
      if (x > 0) {
        math.log((m + 1.0) / (x + 1.0))
      } else {
        0
      }
    })
    assert(model.idf ~== expected absTol 1e-12)

    val assertHelper = (tfidf: Array[Vector]) => {
      assert(tfidf.length === 3)
      val tfidf0 = tfidf(0).asInstanceOf[SparseVector]
      assert(tfidf0.indices === Array(1, 3))
      assert(DenseVector(tfidf0.data) ~==
        DenseVector(1.0 * expected(1), 2.0 * expected(3)) absTol 1e-12)
      val tfidf1 = tfidf(1).asInstanceOf[DenseVector]
      assert(DenseVector(tfidf1.data) ~==
        DenseVector(0.0, 1.0 * expected(1), 2.0 * expected(2), 3.0 * expected(3)) absTol 1e-12)
      val tfidf2 = tfidf(2).asInstanceOf[SparseVector]
      assert(tfidf2.indices === Array(1))
      assert(tfidf2.data(0) ~== (1.0 * expected(1)) absTol 1e-12)
    }
    // Transforms a RDD
    val tfidf = model.transform(termFrequencies).collect()
    assertHelper(tfidf.toArray)
    // Transforms local vectors
    val localTfidf = localTermFrequencies.map(model.transform).toArray
    assertHelper(localTfidf)
  }
}
