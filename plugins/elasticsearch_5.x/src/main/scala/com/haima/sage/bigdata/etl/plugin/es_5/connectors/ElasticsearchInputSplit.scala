package com.haima.sage.bigdata.etl.plugin.es_5.connectors

import com.haima.sage.bigdata.etl.common.model.ES5Source
import com.haima.sage.bigdata.etl.plugin.es_5.driver.ElasticSearchDriver
import org.apache.flink.core.io.InputSplit

case class ElasticsearchInputSplit(indexName: String,
                                   typeName: String,
                                   clusterName: String,
                                   shard: Int,
                                   nodeHost: String,
                                   nodeId: String,
                                   nodePort: Int) extends InputSplit {
  val getSplitNumber = 1
  val es5Source = ES5Source(
    cluster = clusterName,
    hostPorts = Array((nodeHost, nodePort)),
    index = indexName,
    esType = typeName,
    field = "",
    start = "",
    step = 100,
    queryDSL = None
  )

  lazy val driver = ElasticSearchDriver(es5Source)

}
