package com.haima.sage.bigdata.analyzer.timeseries.models

private[timeseries] case class Temp(time: Long, diff: Option[Long], data: Array[Array[(Long, Double)]], groups: Map[String, Any], backend: Map[String, Any])
