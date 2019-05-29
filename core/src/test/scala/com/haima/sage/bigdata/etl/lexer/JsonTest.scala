package com.haima.sage.bigdata.etl.lexer

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class JsonTest extends Mapper{


  @Test
  def fastJson(): Unit ={
import scala.collection.JavaConverters._
   //println( JSON.toJSONString(ArrayBuffer(1,2,3),Seq():_*))
    var a=new util.HashMap[String,String]()


  }


  @Test
  def kafkaTest():Unit={

val data="""{"fields":{"Packets_Sent_persec":0.39999839663505554,"err_out":0},"name":"win_net","tags":{"endpoint":"10.10.100.45","host":"WIN-73T19OAEOAM","instance":"Intel[R] PRO_1000 MT Network Connection","objectname":"Network Interface"},"timestamp":1521503147}""".stripMargin
    val rt = JSONLogLexer().parse(data)
    println(rt("fields").asInstanceOf[java.util.Map[String,Any]].get("Packets_Sent_persec").asInstanceOf[java.math.BigDecimal].doubleValue())
    println(rt("fields").asInstanceOf[java.util.Map[String,Any]].get("err_out").getClass)

    val rt2= mapper.readValue[Map[String,Any]](data)
    println(rt2("fields").asInstanceOf[Map[String,Any]].get("Packets_Sent_persec").getClass)
    println(rt2("fields").asInstanceOf[Map[String,Any]]("err_out").getClass)
  }

  @Test
  def jsonMulti(): Unit ={
    val data ="""{"a":{"b":"c","d":"d"},"a1":"a-data","a2":{"c3":"c2"}}""".stripMargin

    val rt = JSONLogLexer().parse(data)
    println(rt)
    assert(!JSONLogLexer().parse(data).contains("error"))
  }

  @Test
  def jsonWithSpalator(): Unit = {
    val data =
      """
        |﻿{
        |"name":"kkp1",
        |"birth":"1984-07-08",
        |"ceshi":"PASS",
        |"sex":"y",
        |"id":9,
        |"birthaddr":"xitan"
        |}""".stripMargin
    val rt = JSONLogLexer().parse(data)
    println(rt)
    assert(!JSONLogLexer().parse(data).contains("error"))


  }

  @Test
  def parseArray(): Unit = {
    val data ="""{"features":[0.9988811614816755,0.9998931308097386,1.0,1.0,0.0,1.0,1.0,0.0,0.0,1.0,0.0,1.0,0.0,0.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0,1.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0],"label":1}"""
    import scala.collection.JavaConversions._
    JSON.parseObject(data).get("features").asInstanceOf[java.util.List[Any]].toArray().map(_.toString.toDouble).foreach(println)
  }


}
