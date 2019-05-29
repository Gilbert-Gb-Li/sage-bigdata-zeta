package com.haima.sage.bigdata.etl.json


import java.util.UUID

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

/**
  * Created by zhhuiyan on 16/9/19.
  */
class MappingTest extends Mapper {
  @Test
  def map2kv(): Unit ={
    val writer = SwitchWriter(UUID.randomUUID().toString, """field15"""".stripMargin, SwitchType.Match, Array(("aa", ES2Writer("0", "yzh_es", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1000))), ES2Writer("0", "yzh_es", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1000))
    val json = mapper.writeValueAsString(writer)

    val data=mapper.writeValueAsString(Map("V"->"1.2","T"-> "2018_10_26_13_14_09_824","zdykey"->json))
  println(data.substring(1,data.length-1).replaceAll(s"""",""","""" """))
  }


  @Test
  def ds(): Unit = {
    val data =
      """{
        |  "id" : "c40ba2c3-35c6-4546-bee1-4fa12f576766",
        |  "name" : "json-array-test",
        |  "collectorId" : "worker-yzh",
        |  "parserId" : "f59c32bc-7c75-4d4f-bcae-f32764dc8c6e",
        |  "assetTypeId" : "59377c67-2bb2-46e8-8740-fcd2e19f62bf",
        |  "assetTypeName" : "手机银行",
        |  "assetId" : "",
        |  "writers" : [ {
        |    "id" : "904f79ff-8381-4175-be07-a709f5d0e981",
        |    "name" : "本地ES",
        |    "writeType" : null
        |  } ],
        |  "data" : {
        |    "path" : "data/json-array.json",
        |    "category" : null,
        |    "contentType" : "txt",
        |    "encoding" : null,
        |    "codec" : {
        |      "name" : "line"
        |    },
        |    "position" : "END",
        |    "skipLine" : 0,
        |    "name" : "directory"
        |  },
        |  "properties" : {
        |    "polling_ms" : "1000",
        |    "timeout_ms" : "100"
        |  },
        |  "status" : "WORKER_STOPPED"
        |}""".stripMargin
    val wrapper = mapper.readValue[DataSourceWrapper](data)
    val json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(wrapper)
    val rt = mapper.readValue[DataSourceWrapper](json)
    println(rt)


  }

  @Test
  def data(): Unit = {
    val json ="""{"id":"904f79ff-8381-4175-be07-a709f5d0e981","name":"本地ES","writeType":"es","data":{"cluster":"elasticsearch","hostPorts":[["127.0.0.1",9200]],"index":"logs_$","indexType":"log","number_of_shards":5,"number_of_replicas":0,"numeric_detection":false,"date_detection":false,"cache":1000,"name":"es","idfields":"ABC"}}""".stripMargin
    val es = """{"cluster":"elasticsearch","hostPorts":[["127.0.0.1",9200]],"index":"logs_$","indexType":"log","number_of_shards":5,"number_of_replicas":0,"numeric_detection":false,"date_detection":false,"cache":1000,"name":"es","idFields":"ABC"}"""
    val wrapper = mapper.readValue[WriteWrapper](json)
    println(wrapper)
    val wrapper2 = mapper.readValue[ES2Writer](es)
    println(wrapper2)
  }


  @Test
  def switchWriter(): Unit = {
    val writer = SwitchWriter(UUID.randomUUID().toString, "field15", SwitchType.Match, Array(("aa", ES2Writer("0", "yzh_es", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1000))), ES2Writer("0", "yzh_es", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1000))
    val json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(writer)
    println(json)
    val data1 =
      """{
        |"data":{
        |   "name":"switch",
        |   "cache":1000,
        |   "field":"field15",
        |   "type":"Match",
        |   "default":{
        |         "host":"127.0.0.1",
        |         "port":5672,
        |         "queue":"endpoint",
        |         "durable":true,
        |         "username":"raysdata",
        |         "password":"raysdata",
        |         "contentType":{"name":"delkv","delimit":",","tab":"="},
        |         "cache":1000,
        |         "name":"rabbitmq"
        |   },
        |   "writers":[
        |       ["12",{
        |             "cluster":"elasticsearch",
        |             "hostPorts":[["127.0.0.1",9200]],
        |             "index":"logs_$",
        |             "indexType":"logs",
        |             "persisRef":{"name":"none"},
        |             "cache":1000,
        |             "name":"es"
        |             }
        |        ]
        |   ]
        |},
        |"name":"mobilebank"}""".stripMargin
    val data: WriteWrapper = mapper.readValue[WriteWrapper](data1)
    println(data)
  }

  @Test
  def esWriter(): Unit = {
    val writer = ES2Writer("0", "yzh_es", Array(("yzh", 9200)), "logs_%{yyyyMMdd}", "iis", cache = 1000)
    val json = mapper.writeValueAsString(writer)
    println(json)
    val data: Writer = mapper.readValue[Writer](json)
    println(data)
  }

  @Test
  def writerWrapper(): Unit = {

    val json = "{\"name\":\"1\",\"writeType\":\"rabbitmq\"}"
    println(json)
    val data: WriteWrapper = mapper.readValue[WriteWrapper](json)
    println(data)
  }

  @Test
  def sftpSource(): Unit = {
    val source = SFTPSource("127.0.1", Option(22), FileSource("/opt/a"), None)
    val json = mapper.writeValueAsString(source)
    println(json)
    val data = mapper.readValue[DataSource](json)
    println(data)
  }

  @Test
  def parser(): Unit = {
    val source = Delimit(fields = Array("a", "b"))
    val json = mapper.writeValueAsString(source)
    println(json)
    val data = mapper.readValue[Parser[MapRule]](json)
    println(data)
    println(mapper.writeValueAsString(data))
  }

  @Test
  def analyzer(): Unit = {
    val source = ReParser(field = Some("aaa"), parser = Regex("aaa"))


    val json = mapper.writeValueAsString(source)
    println(json)
    val data = mapper.readValue[MapRule](json)
    println(data)
    println(mapper.writeValueAsString(data))
  }


  @Test
  def filter(): Unit = {
    val parser = ReParser(field = Some("aaa"), parser = Regex("aaa"))


    val source = Filter(Array(Drop(), AddFields(Map("a" -> "1")),
      parser
    ))
    val json = mapper.writeValueAsString(source)

    val data = mapper.readValue[Filter](json)
    println(data)
    println(mapper.writeValueAsString(data))
  }

  @Test
  def config(): Unit = {
    /*
    *
    * */
    val config = Config("alert_avcon","testSingle",
      SingleChannel(FileSource("data/视频会议管理was及数据库日志/alert_avcon.log", Option("alert_avcon"),
        codec = None),
        Some(Regex(s"%{DATESTAMP:@timestample}\t%{ALL:LOG}", Array(ReParser(Some("LOG"), Delimit(Some("\t"), Array("message", "infos"))),
          ReParser(Some("infos"), DelimitWithKeyMap(Some("\t"), Some(": "))))))),
      Some(Collector("73618a27-4402-416e-9417-2ba2a0b6a0cb", "localhost", 5151)), List(ES2Writer("1", "yzh_es", Array(("yzh",9200)), "logs_web_reputation", "iis", cache = 1000)))

    val json = mapper.writeValueAsString(config)
    println(json)
    val data = mapper.readValue[Config](json)
    println(data)
    println(mapper.writeValueAsString(data))
  }
}
