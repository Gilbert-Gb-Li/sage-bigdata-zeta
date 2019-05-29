package com.haima.sage.bigdata.analyzer.timeseries.stats

import com.haima.sage.bigdata.analyzer.timeseries.models.ARModel
import org.apache.commons.math3.random.MersenneTwister
import org.apache.flink.ml.math.DenseVector
import org.scalatest.FunSuite
/**
  * Created by CaoYong on 2017/10/25.
  */
class AugmentedDickeyFullerSuite extends FunSuite {
  test("non-stationary AR model") {
    val rand = new MersenneTwister(10L)
    val arModel = new ARModel(0.0, .95)
    val sample = arModel.sample(500, rand)

    val (adfStat, pValue) = TimeSeriesStatisticalTests.adftest(sample, 1)
    assert(!java.lang.Double.isNaN(adfStat))
    assert(!java.lang.Double.isNaN(pValue))
    println("adfStat: " + adfStat)
    println("pValue: " + pValue)
  }

  test("iid samples") {
    val rand = new MersenneTwister(11L)
    val iidSample = Array.fill(500)(rand.nextDouble())
    val (adfStat, pValue) = TimeSeriesStatisticalTests.adftest(new DenseVector(iidSample), 1)
    assert(!java.lang.Double.isNaN(adfStat))
    assert(!java.lang.Double.isNaN(pValue))
    println("adfStat: " + adfStat)
    println("pValue: " + pValue)
  }
}
