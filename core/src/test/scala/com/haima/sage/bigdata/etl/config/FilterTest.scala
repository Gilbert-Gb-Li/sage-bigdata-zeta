package com.haima.sage.bigdata.etl.config

import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.common.model.{NothingParser, Parser}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

/**
  * Created by zhhuiyan on 2016/10/11.
  */
class FilterTest extends Mapper {


  @Test
  def fieldCut(): Unit ={
    val  filter =Filter(Array(FieldCut("a",0,10)))
    println(mapper.writeValueAsString(FieldCut("a",0,10)))
    println(mapper.writeValueAsString(filter))
  }
  @Test
  def fieldCutMath(): Unit ={
    val  fieldcut =FieldCut("a",0,10)
   assert(fieldcut.isInstanceOf[MapFieldRule])
  }

  @Test
  def ScriptFilterTest(): Unit ={
    val scriptFilter = MapScriptFilter("event.get(\"a\")","js", Array(MapCase("a", AddFields(Map("c" -> "1")))))

    val json =mapper.writeValueAsString(scriptFilter)
    println(json)
    println(mapper.readValue[MapScriptFilter](json))

    val ddd=
      """[ [ "adasdas" ], {
        |  "filter" : [ {
        |    "fields" : {
        |      "a" : "b"
        |    },
        |    "name" : "addFields"
        |  }, {
        |    "script" : "event.get(\"a\")",
        |    "type" : "js",
        |    "cases" : [ {
        |      "value" : "a",
        |      "rule" : {
        |        "fields" : {
        |          "c" : "1"
        |        },
        |        "name" : "addFields"
        |      },
        |      "name" : "case"
        |    } ],
        |    "name" : "scriptFilter"
        |  } ],
        |  "name" : "nothing"
        |} ]""".stripMargin
    val json2=
      """[ [ "adasdas" ], {
        |  "name" : "nothing",
        |  "filter" : [ {
        |    "fields" : {
        |      "a" : "b"
        |    },
        |    "name" : "addFields"
        |  }, {
        |    "script" : "event.get(\"a\")",
        |    "type":"js",
        |    "cases" : [ {
        |      "value" : "a",
        |      "rule" : {
        |        "fields" : {
        |          "c" : "1"
        |        },
        |        "name" : "addFields"
        |      },
        |      "name" : "case"
        |    } ],
        |    "name" : "scriptFilter"
        |  } ]
        |} ]""".stripMargin

    val data=(List("adasdas"),NothingParser(filter = Array(AddFields(Map("a"->"b")),scriptFilter)))
    val json3=mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data)
    println(json3)
    println(mapper.readValue[(List[String], Parser[MapRule])](ddd))
  }

}
