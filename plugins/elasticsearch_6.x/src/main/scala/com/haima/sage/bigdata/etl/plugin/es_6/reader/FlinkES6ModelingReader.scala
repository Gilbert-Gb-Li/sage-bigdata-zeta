package com.haima.sage.bigdata.etl.plugin.es_6.reader

import com.haima.sage.bigdata.analyzer.modeling.reader.FlinkModelingReader
import com.haima.sage.bigdata.etl.common.model.{ES6Source, RichMap, SingleChannel}
import com.haima.sage.bigdata.etl.plugin.es_6.connectors.ElasticsearchDataSet
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * Modeling Reader for Elasticsearch 5
  *
  * @param channel
  */
class FlinkES6ModelingReader(override val channel: SingleChannel) extends FlinkModelingReader {
  override def getDataSet(evn: ExecutionEnvironment): DataSet[RichMap] = {
    ElasticsearchDataSet.fromElasticsearchQuery(evn, channel.dataSource.asInstanceOf[ES6Source])
  }
}
