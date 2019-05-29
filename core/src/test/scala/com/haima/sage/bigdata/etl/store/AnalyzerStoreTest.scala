package com.haima.sage.bigdata.etl.store

import com.haima.sage.bigdata.etl.common.model.AnalyzerWrapper
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test
import org.mockito.Mockito.{mock, when}

/**
  * Created by zhhuiyan on 2017/5/17.
  */
class AnalyzerStoreTest extends Mapper {
  private lazy val store: DBAnalyzerStore = mock(classOf[DBAnalyzerStore])
  val json = s"""{"id":"123","name":"handle1","sample":"1,2,3","data":{"fromFields":[["a","string"],["b","int"],["c","string"]],"table":"test","sql":"select a,c from test","toFields":["a","c"],"name":"sql"}}""".stripMargin
  val json2 = s"""{"id":"1234","name":"handle2","sample":"1,2,3","data":{"fromFields":[["a","string"],["b","int"],["c","string"]],"table":"test","sql":"select a,c from test","toFields":["a","c"],"name":"sql"}}""".stripMargin

  private lazy val handleWrapper = mapper.readValue[AnalyzerWrapper](json)

  {
    val analyzerWrapper1 = mapper.readValue[AnalyzerWrapper](json)
    val analyzerWrapper2 = mapper.readValue[AnalyzerWrapper](json2)

    when(store.all()).thenReturn(List(analyzerWrapper1, analyzerWrapper2))
    when(store.get("123")).thenReturn(Some(analyzerWrapper1))
    when(store.set(handleWrapper)).thenReturn(true)
  }



  @Test
  def data(): Unit = {
    val json = s"""{"id":"123","name":"handle1","sample":"1,2,3","data":{"fromFields":[["a","string"],["b","int"],["c","string"]],"table":"test","sql":"select a,c from test","toFields":["a","c"],"name":"sql"}}""".stripMargin

    println(mapper.readValue[AnalyzerWrapper](json))
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(handleWrapper))

  }

  @Test
  def add(): Unit = {


    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(handleWrapper))

    println(handleWrapper.data.orNull.filter)
    assert(store.set(handleWrapper), "add handle must be true")

  }

  @Test
  def list(): Unit = {

    assert(store.all().size == 2, " handle all must running")

  }

  @Test
  def oneIdel(): Unit = {

//    val analyzer = SQLAnalyzer("flink://", Array(("", "")), "tb1", "", Array(""), Some("logtime"), Some(1))
//
//    val analyzer2 = mapper.readValue[SQLAnalyzer](mapper.writeValueAsString(analyzer)).copy()
//    println("clazz is " + analyzer.idleStateRetentionTime.orNull)
//    //  assert(analyzer.idleStateRetentionTime.get.getClass ==classOf[java.lang.Long])
//    println("clazz is " + analyzer2)
//    //  assert(analyzer2.idleStateRetentionTime.get.getClass ==classOf[java.lang.Integer])
  }


  @Test
  def get(): Unit = {

    assert(store.get("123") match {
      case Some(handle) =>
        println(handle)
        true
      case _ =>
        false
    }, "after add must not null ")

  }

}
