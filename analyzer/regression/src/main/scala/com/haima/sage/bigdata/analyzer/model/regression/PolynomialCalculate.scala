package com.haima.sage.bigdata.analyzer.model.regression

import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{Vector, VectorBuilder}

/** Maps a vector into the polynomial feature space.
  *
  * This transformer takes a a vector of values `(x, y, z, ...)` and maps it into the
  * polynomial feature space of degree `d`. That is to say, it calculates the following
  * representation:
  *
  * `(x, y, z, x^2, xy, y^2, yz, z^2, x^3, x^2y, x^2z, xyz, ...)^T`
  *
  * This transformer can be prepended to all [[org.apache.flink.ml.pipeline.Transformer]] and
  * [[org.apache.flink.ml.pipeline.Predictor]] implementations which expect an input of
  * [[LabeledVector]].
  *
  * @example
  * {{{
  *             val trainingDS: DataSet[LabeledVector] = ...
  *
  *             val polyFeatures = PolynomialFeatures()
  *               .setDegree(3)
  *
  *             val mlr = MultipleLinearRegression()
  *
  *             val pipeline = polyFeatures.chainPredictor(mlr)
  *
  *             pipeline.fit(trainingDS)
  *          }}}
  *
  * =Parameters=
  *
  *  - [[org.apache.flink.ml.preprocessing.PolynomialFeatures.Degree]]: Maximum polynomial degree
  */


object PolynomialCalculate extends Serializable {

  def calculatePolynomial[T <: Vector : VectorBuilder](degree: Int, vector: T): T = {
    val builder = implicitly[VectorBuilder[T]]
    builder.build(calculateCombinedCombinations(degree, vector))
  }

  /** Calculates for a given vector its representation in the polynomial feature space.
    *
    * @param degree Maximum degree of polynomial
    * @param vector Values of the polynomial variables
    * @return List of polynomial values
    */
  private def calculateCombinedCombinations(degree: Int, vector: Vector): List[Double] = {
    if (degree == 0) {
      List()
    } else {
      val partialResult = calculateCombinedCombinations(degree - 1, vector)

      val combinations = calculateCombinations(vector.size, degree)

      val result = combinations map {
        combination =>
          combination.zipWithIndex.map {
            case (exp, idx) => math.pow(vector(idx), exp)
          }.fold(1.0)(_ * _)
      }

      result ::: partialResult
    }

  }

  /** Calculates all possible combinations of a polynom of degree `value`, whereas the polynom
    * can consist of up to `length` factors. The return value is the list of the exponents of the
    * individual factors
    *
    * @param length maximum number of factors
    * @param value  degree of polynomial
    * @return List of lists which contain the exponents of the individual factors
    */
  private def calculateCombinations(length: Int, value: Int): List[List[Int]] = {
    if (length == 0) {
      List()
    } else if (length == 1) {
      List(List(value))
    } else {
      value to 0 by -1 flatMap {
        v =>
          calculateCombinations(length - 1, value - v) map {
            v :: _
          }
      } toList
    }
  }
}
