package com.haima.sage.bigdata.analyzer.timeseries.models

import java.util.Random

import org.apache.commons.math3.random.MersenneTwister
import org.apache.flink.ml.math.DenseVector
import org.scalatest.FunSuite
import com.haima.sage.bigdata.analyzer.ml.utils.MatrixUtils._

/**
  * Created by CaoYong on 2017/10/29.
  */
class AutoregressionSuite extends FunSuite {
  test("fit AR(1) model") {
    val model = new ARModel(1.5, Array(.2))
    val ts = model.sample(5000, new MersenneTwister(10L))
    val fittedModel = Autoregression.fitModel(ts, 1)
    assert(fittedModel.coefficients.length == 1)
    assert(math.abs(fittedModel.c - 1.5) < .07)
    assert(math.abs(fittedModel.coefficients(0) - .2) < .03)
  }

  test("fit AR(2) model") {
    val model = new ARModel(1.5, Array(.2, .3))
    val ts = model.sample(5000, new MersenneTwister(10L))
    val fittedModel = Autoregression.fitModel(ts, 2)
    assert(fittedModel.coefficients.length == 2)
    assert(math.abs(fittedModel.c - 1.5) < .15)
    assert(math.abs(fittedModel.coefficients(0) - .2) < .03)
    assert(math.abs(fittedModel.coefficients(1) - .3) < .03)
  }

  test("add and remove time dependent effects") {
    val rand = new Random()
    val ts = new DenseVector(Array.fill(1000)(rand.nextDouble()))
    val model = new ARModel(1.5, Array(.2, .3))
    val added = model.addTimeDependentEffects(ts, DenseVector.zeros(ts.size))
    val removed = model.removeTimeDependentEffects(added, DenseVector.zeros(ts.size))
    assert((toBreeze(ts) - toBreeze(removed)).toArray.forall(math.abs(_) < .001))
  }
}
