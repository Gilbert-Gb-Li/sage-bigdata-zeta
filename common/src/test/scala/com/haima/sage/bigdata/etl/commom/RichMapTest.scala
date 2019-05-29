package com.haima.sage.bigdata.etl.commom

import java.util

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

class RichMapTest extends Mapper {

  @Test
  def get(): Unit = {

    val data = RichMap(Map("b.c" -> "v", "c" -> Map("d" -> 2)))
    val d2 = data + ("batch" -> 1) + ("d.f" -> 3)
    println(d2.get("batch"))
    println(d2.get("b.c"))
    println(d2.get("c.d"))
    println(d2.get("d.f"))
    val d3 = d2.filter(d => true)
    println(d3.getClass)
  }

  @Test
  def remove(): Unit = {
    import scala.collection.JavaConversions._
    val data = Map("a" -> Map("b" -> "v", "c" -> "v2"), "d" -> Map("e" -> "v3"))

    val rich1 = RichMap(data.toMap)
    val rich = rich1 - "a.b"


    assert(rich.get("a").orNull.isInstanceOf[java.util.Map[_, _]])
    assert(rich.complex().get("d").orNull.asInstanceOf[Map[_, _]].size() == 1)
    assert(rich.-("d.e").size() == 1)

  }

  @Test
  def complex(): Unit = {
    import scala.collection.JavaConversions._
    val data = new util.HashMap[String, Any]()
    data.put("a.b.c", "v")
    data.put("a.c", "v2")


    val rich = RichMap(data.toMap)

    assert(rich.complex().size == 1)

  }

  @Test
  def putArray(): Unit = {
    /*com.haima.sage.bigdata.etl.server.Master$ -
    Map(query -> {bool=Map(filter -> List(Map(term -> Map(status -> published)))), bool2={filter=Map(terms -> Map(endpoint -> List(1, 2)))}}*/
    val data = Map("query" -> Map("bool" -> Map("filter" -> List(Map("terms" -> Map("endpoint" -> List("a", "b"))),
      Map("term" -> Map("status" -> "published")),
      Map("term" -> Map("status2" -> "published"))))))

    val rich1 = RichMap(data)
    val rich = rich1.+("query.bool.filter" -> Map("terms" -> Map("endpoint" -> List(1, 2))))

    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rich))

    val data1 = rich.get("query.bool.filter").orNull

    assert(rich.get("query.bool.filter").orNull.asInstanceOf[List[_]].size == 4)


  }

  @Test
  def putArrayMap(): Unit = {
    /*com.haima.sage.bigdata.etl.server.Master$ -
    Map(query -> {bool=Map(filter -> List(Map(term -> Map(status -> published)))), bool2={filter=Map(terms -> Map(endpoint -> List(1, 2)))}}*/
    val data = Map("query" -> Map("bool" -> Map("filter" -> List(Map("term" -> Map("status" -> "published")), Map("term" -> Map("status2" -> "published"))))))

    val rich1 = RichMap(data)


    val rich = rich1.+("query.bool.filter.terms" -> Map("endpoint" -> List(1, 2)))

    //    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rich.real))
    //
    //    println(rich.get("query.bool.filter").orNull.getClass)
    //    println(rich.get("query.bool.filter").orNull.asInstanceOf[List[_]])

    assert(rich.get("query.bool.filter").orNull.asInstanceOf[List[_]].size == 2)
    println(rich.get("query.bool.filter.terms").orNull.getClass)
    assert(rich.get("query.bool.filter.terms").orNull.asInstanceOf[java.util.List[_]].size == 2)


  }

  @Test
  def putMap(): Unit = {
    /*com.haima.sage.bigdata.etl.server.Master$ -
    Map(query -> {bool=Map(filter -> List(Map(term -> Map(status -> published)))), bool2={filter=Map(terms -> Map(endpoint -> List(1, 2)))}}*/
    val data = Map("query" -> Map("bool" -> Map("filter" ->
      List(Map("term" ->
        Map("status" -> "published"))))))

    val rich1 = RichMap(data)

    val rich = rich1.+("query.bool.filter2.terms" -> Map("endpoint" -> List(1, 2)))

    assert(rich.get("query.bool.filter").orNull.asInstanceOf[List[_]].size == 1)
    assert(rich.get("query.bool.filter2").orNull.asInstanceOf[java.util.Map[_, _]].size == 1)

  }

}
