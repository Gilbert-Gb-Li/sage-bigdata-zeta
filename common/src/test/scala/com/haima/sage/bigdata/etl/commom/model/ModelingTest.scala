package com.haima.sage.bigdata.etl.commom.model

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

/**
  * Created by evan on 17-8-7.
  **/
class ModelingTest extends Mapper {

  @Test
  def modelConfigTest(): Unit = {
    val wrapper = ModelingWrapper(
      id = Some("1"),
      name = "建模任务1",
      `type` = Some(AnalyzerChannel("127.0.0.1:6666")),
      channel = "ds-es",
      analyzer = Some("a-sql"),
      sinks = List("w-es"),
      status = Status.PENDING,
      worker = Some("zhangshuyu"),
      properties = None
    )
    val json = mapper.writeValueAsString(wrapper)
    println(json)
    /*val wrapper = ModelingWrapper(Option("123"), "test", Option("sample"), Option(modeling), Option(sink), Option(source))

    val json = mapper.writeValueAsString(wrapper)
    println(json)

    val data = mapper.readValue[ModelingWrapper](json)

    println(mapper.writeValueAsString(data))*/
  }
}
