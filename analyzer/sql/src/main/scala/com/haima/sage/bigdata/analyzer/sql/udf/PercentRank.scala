package com.haima.sage.bigdata.analyzer.sql.udf

import org.apache.commons.math3.exception.OutOfRangeException
import org.apache.flink.table.functions.AggregateFunction

import scala.collection.mutable

/** The initial accumulator for Sum aggregate function */
case class PercentRankAccumulator(data: mutable.ArrayBuffer[Double], var percent: Double = 0.5)

class PercentRank extends AggregateFunction[Double, PercentRankAccumulator] {

  override def createAccumulator(): PercentRankAccumulator = {
    PercentRankAccumulator(mutable.ArrayBuffer())
  }

  def accumulate(acc: PercentRankAccumulator, value: AnyVal, percent: Double): Unit = {
    if (percent > 1 || percent < 0) {
      throw new OutOfRangeException(percent, 0.01, 0.99)
    }
    acc.data += value.toString.toDouble
    acc.percent = percent

  }

  def retract(acc: PercentRankAccumulator, value: AnyVal, percent: Double): Unit = {
    acc.data - value.toString.toDouble
  }

  def merge(acc: PercentRankAccumulator, it: java.lang.Iterable[PercentRankAccumulator]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.data ++= a.data
      acc.percent = a.percent
    }
  }

  def resetAccumulator(acc: PercentRankAccumulator): Unit = {
    acc.data.clear()
  }

  override def getValue(accumulator: PercentRankAccumulator): Double = {
    if (accumulator.data.size <= 0) {
      0.asInstanceOf[Double]
    } else {
      val sorted = accumulator.data.sorted
      val max = Math.floor(accumulator.data.size * (accumulator.percent + 0.01)).toInt
      val min = Math.ceil((accumulator.data.size - 1) * (accumulator.percent - 0.01)).toInt


      if (max == min) {
        sorted(max)
      } else if (max - min == 1 && max >= sorted.size) {

        sorted(min)

      } else if (max - min == 1) {

        (sorted(max) + sorted(min)) / 2

      } else {
        val data = (min + 1).until(max).map(sorted(_))
        data.sum / data.size

      }

    }
  }


}
