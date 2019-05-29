package com.haima.sage.bigdata.etl.json

import com.haima.sage.bigdata.etl.common.model.{DataSource, RabbitMQSource}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

/**
  * Created by zhhuiyan on 2016/10/18.
  */
class RabbitMQConfigTest extends Mapper {
  @Test
  def toJsonString(): Unit = {
    val ds:DataSource= RabbitMQSource()
   println(mapper.writeValueAsString(ds))
    println(mapper.readValue[RabbitMQSource](mapper.writeValueAsString(ds)))

  }

}
