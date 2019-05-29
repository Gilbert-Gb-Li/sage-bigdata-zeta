package com.haima.sage.bigdata.analyzer.modeling.reader

import com.haima.sage.bigdata.etl.common.model.{NetSource, RichMap, SingleChannel}
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation, getCallLocationName}

/**
  * Created by evan on 17-8-30.
  *
  * Modeling readers
  *
  */
class FlinkAkkaModelingReader(override val channel: SingleChannel) extends FlinkModelingReader {

  def getDataSet(evn: ExecutionEnvironment): DataSet[RichMap] = {
    val inputFormat = new AkkaInputFormat(channel.dataSource.asInstanceOf[NetSource])
    new DataSet[RichMap](new DataSource[RichMap](
      evn.getJavaEnv,
      inputFormat,
      createTypeInformation[RichMap],
      getCallLocationName()))

  }
}
