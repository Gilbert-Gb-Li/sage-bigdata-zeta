package com.haima.sage.bigdata.etl.store

/**
  * Created by Dell on 2017-07-27.
  */
import com.haima.sage.bigdata.etl.knowledge.Knowledge
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test
import org.mockito.Mockito.{mock, when}

class KnowledgeStoreTest extends Mapper{
  private lazy val store:KnowledgeStore=mock(classOf[KnowledgeStore])
  val json=
    s"""{"id":"101",
        "name":"test1",
        "assetType":"root",
        "driver":"72394efe-67dd-4323-bdfd-526b18862779",
        "collector":"bb9c07be-f67d-445b-a76c-1028edc5885a",
        "parser":"76a155b6-fe72-4827-b41f-01e2e239f080"}""".stripMargin
  val json2=
    s"""{"id":"102",
        "name":"test2",
        "assetType":"root",
        "driver":"fb49ca1f-cb25-4746-b8bc-41135532543d",
        "collector":"bb9c07be-f67d-445b-a76c-1028edc5885a",
        "parser":"5b49ce41-1131-4af1-aa50-b48008afbba4"}""".stripMargin
  private lazy val handleWrapper=mapper.readValue[Knowledge](json)

  {
    val knowledge1=mapper.readValue[Knowledge](json)
    val knowledge2=mapper.readValue[Knowledge](json2)
    when(store.all()).thenReturn(List(knowledge1,knowledge2))
    when(store.get("101")).thenReturn(Some(knowledge1))
    when(store.set(handleWrapper)).thenReturn(true)
  }

  @Test
  def data():Unit={
    val json=
      s"""{"id":"101",
        "name":"test1",
        "assetType":"root",
        "driver":"72394efe-67dd-4323-bdfd-526b18862779",
        "collector":"bb9c07be-f67d-445b-a76c-1028edc5885a",
        "parser":"76a155b6-fe72-4827-b41f-01e2e239f080"}""".stripMargin
    println(mapper.readValue[Knowledge](json))
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(handleWrapper))
  }

  @Test
  def add():Unit={
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(handleWrapper))
    assert(
      store.set(handleWrapper), "add handle must be true")

  }

  @Test
  def list():Unit={
    assert(store.all().size == 2, " handle all must running")
    println(store.all().size)
    println(store.all())
  }

}
