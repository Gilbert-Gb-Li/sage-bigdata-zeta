package com.haima.sage.bigdata.analyzer.timeseries.models

import com.haima.sage.bigdata.analyzer.timeseries.Lag
import com.haima.sage.bigdata.analyzer.ml.utils.MatrixUtils.matToRowArrs
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.flink.ml.math.{Breeze, DenseVector, Vector}
/**
  * Created by CaoYong on 2017/10/24.
  */
object Autoregression {
  /**
    * Fits an AR(1) model to the given time series
    */
  def fitModel(ts: Vector): ARModel = fitModel(ts, 1)

  /**
    * Fits an AR(n) model to a given time series
    * @param ts Data to fit
    * @param maxLag The autoregressive factor, terms t - 1 through t - maxLag are included
    * @param noIntercept A boolean to indicate if the regression should be run without an intercept,
    *                    the default is set to false, so that the OLS includes an intercept term
    * @return AR(n) model
    */
  def fitModel(ts: Vector, maxLag: Int, noIntercept: Boolean = false): ARModel = {
    // This is loosely based off of the implementation in statsmodels:
    // https://github.com/statsmodels/statsmodels/blob/master/statsmodels/tsa/ar_model.py

    // Make left hand side
    val Y = Breeze.Vector2BreezeConverter(ts).asBreeze(maxLag until ts.size)
    // Make lagged right hand side
    val X = Lag.lagMatTrimBoth(ts, maxLag)

    val regression = new OLSMultipleLinearRegression()
    regression.setNoIntercept(noIntercept) // drop intercept in regression
    regression.newSampleData(Y.toArray, matToRowArrs(X))
    val params = regression.estimateRegressionParameters()
    val (c, coeffs) = if (noIntercept) (0.0, params) else (params.head, params.tail)
    new ARModel(c, coeffs)
  }
}

class ARModel(val c: Double, val coefficients: Array[Double]) extends TimeSeriesModel {

  def this(c: Double, coef: Double) = this(c, Array(coef))

  def removeTimeDependentEffects(
                                  ts: Vector,
                                  destTs: Vector = null): Vector = {
    val dest = if (destTs == null) new Array[Double](ts.size) else destTs.map(_._2).toArray[Double]
    var i = 0
    while (i < ts.size) {
      dest(i) = ts(i)- c
      var j = 0
      while (j < coefficients.length && i - j - 1 >= 0) {
        dest(i) -= ts(i - j - 1) * coefficients(j)
        j += 1
      }
      i += 1
    }
    new DenseVector(dest)
  }

  def addTimeDependentEffects(ts: Vector, destTs: Vector): Vector = {
    val dest = if (destTs == null) new Array[Double](ts.size) else destTs.map(_._2).toArray[Double]
    var i = 0
    while (i < ts.size) {
      dest(i) = c + ts(i)
      var j = 0
      while (j < coefficients.length && i - j - 1 >= 0) {
        dest(i) += dest(i - j - 1) * coefficients(j)
        j += 1
      }
      i += 1
    }
    new DenseVector(dest)
  }

  def sample(n: Int, rand: RandomGenerator): Vector = {
    val vec = new DenseVector(Array.fill[Double](n)(rand.nextGaussian()))
    addTimeDependentEffects(vec, vec)
  }

  override def forecast(ts: Vector, future: Int): Vector = {
    throw new NotImplementedError("AR forecast not implemented ")
  }
}
