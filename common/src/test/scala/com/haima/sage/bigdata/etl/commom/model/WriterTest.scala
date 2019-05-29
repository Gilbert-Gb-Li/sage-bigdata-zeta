package com.haima.sage.bigdata.etl.commom.model

import com.haima.sage.bigdata.etl.common.model.{ES2Writer, ES5Writer}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

class WriterTest extends Mapper {
  @Test
  def esToString(): Unit = {
    val writer = new ES2Writer("1", "elasticsearch", Array(("127.0.0.1",9200))){
      override val name: String = "es2"
    }
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(writer))
  }

  @Test
  def esfromString(): Unit = {
    val json =
      """{
        |  "id" : "1",
        |  "cluster" : "elasticsearch",
        |  "hostPorts" : [ [ "127.0.0.1",9200 ] ],
        |  "index" : "logs_3333",
        |  "indexType" : "logs_11",
        |  "routingField" : null,
        |  "parentField" : null,
        |  "asChild" : null,
        |  "idFields" : null,
        |  "number_of_shards" : 5,
        |  "number_of_replicas" : 0,
        |  "numeric_detection" : false,
        |  "date_detection" : false,
        |  "persisRef" : null,
        |  "metadata" : null,
        |  "cache" : 1000,
        |  "uri" : "elasticsearch://elasticsearch@127.0.0.1:9200/logs_$/logs",
        |  "name" : "es5"
        |}""".stripMargin

    val writer = mapper.readValue[ES5Writer](json)
    println(writer)
    println(writer.name)
  }


}
