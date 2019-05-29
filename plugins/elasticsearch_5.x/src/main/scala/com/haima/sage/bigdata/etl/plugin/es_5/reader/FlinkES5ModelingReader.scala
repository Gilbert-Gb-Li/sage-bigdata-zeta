package com.haima.sage.bigdata.etl.plugin.es_5.reader

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.{ES5Source, SingleChannel}
import com.haima.sage.bigdata.etl.modeling.flink.reader.FlinkModelingReader
import com.haima.sage.bigdata.etl.plugin.es_5.connectors.ElasticsearchDataSet
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * Modeling Reader for Elasticsearch 5
  *
  * @param channel
  */
class FlinkES5ModelingReader(override val channel: SingleChannel) extends FlinkModelingReader {
  override def getDataSet(evn: ExecutionEnvironment): DataSet[RichMap] = {
    ElasticsearchDataSet.fromElasticsearchQuery(evn, channel.dataSource.asInstanceOf[ES5Source])
  }
}
