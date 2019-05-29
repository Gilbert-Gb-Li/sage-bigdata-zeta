package com.haima.sage.bigdata.etl.utils

import java.sql.Timestamp
import java.util.Date

import com.haima.sage.bigdata.etl.lexer.JSONLogLexer
import com.haima.sage.bigdata.etl.plugin.es_5.utils.ScalaXContentBuilder
import org.junit.Test

class XContentBuilderTest extends Mapper{

  @Test
  def kafkaTest():Unit={
    val builder= new ScalaXContentBuilder()

    val data="""{"fields":{"Packets_Sent_persec":0.39999839663505554,"err_out":0},"array":[1,2,4.01],"name":"win_net","tags":{"endpoint":"10.10.100.45","host":"WIN-73T19OAEOAM","instance":"Intel[R] PRO_1000 MT Network Connection","objectname":"Network Interface"},"timestamp":1521503147}""".stripMargin
    val rt = JSONLogLexer().parse(data)
  //  println( "fast:"+ builder.map(rt).bytes().utf8ToString())
    val rt2= mapper.readValue[Map[String,Any]](data)
    val rt3=    rt2 + ("time"->new Timestamp(new Date().getTime))
   println( "jackson:"+  builder.map(rt3).prettyPrint().bytes().utf8ToString())
  }

}
