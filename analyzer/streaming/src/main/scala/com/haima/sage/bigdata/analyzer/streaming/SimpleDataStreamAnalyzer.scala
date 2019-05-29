package com.haima.sage.bigdata.analyzer.streaming

import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/**
  * Created by ChengJi on 2017/4/25.
  */
abstract class SimpleDataStreamAnalyzer[CONF <: Analyzer, T >: Null <: Any] extends DataStreamAnalyzer[CONF, RichMap, RichMap, T] {

  final def toIntermediate(data: DataStream[RichMap]): DataStream[RichMap] = data

  final def fromIntermediate(data: DataStream[RichMap]): DataStream[RichMap] = data

  override def marshaller: TypeInformation[RichMap] = createTypeInformation[RichMap]
}
