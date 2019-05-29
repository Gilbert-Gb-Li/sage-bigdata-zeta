package com.haima.sage.bigdata.analyzer.timeseries.models

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import org.apache.flink.ml.math.Vector

/**
  * Created by CaoYong on 2017/10/24.
  */
/**
  * Models time dependent effects in a time series.
  */
@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[ARIMAModel], name = "arima"),
  new Type(value = classOf[HoltWintersModel], name = "holtwinters")
))
trait TimeSeriesModel extends Serializable {
  val name: String = this.getClass.getSimpleName.replaceAll("Model", "").toLowerCase

  /**
    * Takes a time series that is assumed to have this model's characteristics and returns a time
    * series with time-dependent effects of this model removed.
    *
    * This is the inverse of [[TimeSeriesModel#addTimeDependentEffects]].
    *
    * @param ts   Time series of observations with this model's characteristics.
    * @param dest Array to put the filtered series, can be the same as ts.
    * @return The dest series, for convenience.
    */
  def removeTimeDependentEffects(ts: Vector, dest: Vector = null): Vector

  /**
    * Takes a series of i.i.d. observations and returns a time series based on it with the
    * time-dependent effects of this model added.
    *
    * @param ts   Time series of i.i.d. observations.
    * @param dest Array to put the filtered series, can be the same as ts.
    * @return The dest series, for convenience.
    */
  def addTimeDependentEffects(ts: Vector, dest: Vector = null): Vector

  def forecast(ts: Vector, future: Int): Vector
}
