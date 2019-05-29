package com.haima.sage.bigdata.etl.plugin.es_5.connectors

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.ES5Source
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
                             es5Source: ES5Source
                            ): DataSet[RichMap] = {
    val marshaller = createTypeInformation[RichMap]
    val inputFormat = new ElasticseachInputFormat(es5Source)
    new DataSet[RichMap](new DataSource[RichMap](env.getJavaEnv,
      inputFormat, marshaller, getCallLocationName()))

  }
}

