package com.haima.sage.bigdata.etl.plugin.es_6.connectors

import com.haima.sage.bigdata.etl.common.model.{ES6Source, RichMap}
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.scala._

object ElasticsearchDataSet {

  /**
    * Creates a dataset from the given query. Simplified queries are not supported. Queries with aggregations are not supported. Queries must include a 'fields' list.
    * Fields will be deserialized in the corresponding fields of the tuple or case class, in the same order. They will be mapped in the corresponding pojoFields in the order
    * they appear in pojoFields.
    *
    * The query can only target a single index.
    *
    * @param env The Flink Scala execution environment.
    * @return A Flink Scala DataSet.
    */
  def fromElasticsearchQuery(env: ExecutionEnvironment,
                             esSource: ES6Source
                            ): DataSet[RichMap] = {
    val marshaller = createTypeInformation[RichMap]
    val inputFormat = new ElasticseachInputFormat(esSource)
    new DataSet[RichMap](new DataSource[RichMap](env.getJavaEnv,
      inputFormat, marshaller, getCallLocationName()))

  }
}

