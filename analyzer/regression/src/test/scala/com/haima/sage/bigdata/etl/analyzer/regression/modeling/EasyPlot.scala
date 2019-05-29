package com.haima.sage.bigdata.analyzer.regression.modeling

import breeze.plot._
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.flink.ml.math._

/**
  * Created by CaoYong on 2017/10/31.
  */
object EasyPlot {
  def ezplot(vec: Vector, style: Char): Figure = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot((0 until vec.size).map(_.toDouble).toArray, vector2Array(vec), style = style)
    f
  }

  def ezplot(vec1: Vector, vec2: Vector, style: Char): Figure = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot((0 until vec1.size).map(_.toDouble).toArray, vector2Array(vec1), style = style)
    p += plot((0 until vec2.size).map(_.toDouble).toArray, vector2Array(vec2), style = style)
    f
  }

  def ezplot(vec: Vector): Figure = ezplot(vec, '-')

  def ezplot(arr: Array[Double], style: Char): Figure = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot(arr.indices.map(_.toDouble).toArray, arr, style = style)
    f
  }

  def ezplot(arr: Array[Double]): Figure = ezplot(arr, '-')

  def ezplot(vecs: Seq[Vector], style: Char): Figure = {
    val f = Figure()
    val p = f.subplot(0)
    val first = vecs.head
    vecs.foreach { vec =>
      p += plot((0 until first.size).map(_.toDouble).toArray, vector2Array(vec), style)
    }
    f
  }

  def ezplot(vecs: Seq[Vector]): Figure = ezplot(vecs, '-')


}
