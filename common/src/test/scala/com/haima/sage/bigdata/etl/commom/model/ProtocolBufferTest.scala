package com.haima.sage.bigdata.etl.commom.model


import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.{Charset, CharsetEncoder}
import java.util.Base64
import java.util.zip.GZIPInputStream

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.protobuf.ByteString
import com.haima.sage.bigdata.etl.common.model.CrawlDataOuterClass.CrawlData
import com.haima.sage.bigdata.etl.common.model.filter.Gunzip
import org.junit.Test

class ProtocolBufferTest {


  def dynamic(): Array[Byte] = {

    val builder1 = CrawlData.newBuilder()
    builder1.setAppPackageName("app.p.n")
    builder1.setAppVersion("1.0")

    builder1.setSchema("hhh")
    builder1.setResourceKey("resourcekey")
    builder1.setCloudServiceId("csid")
    builder1.setContainerId("cid")
    builder1.setDataSource("ds")
    builder1.setDataType(1)
    builder1.setSpiderVersion("1.0")
    val contextb = CrawlData.Content.newBuilder()
    contextb.setTimestamp(1000000001)
    contextb.setData(ByteString.copyFromUtf8("c1"))
    builder1.addContent(contextb.build())
    contextb.clearData()
    contextb.setTimestamp(1000000002)
    contextb.setData(ByteString.copyFromUtf8("c2"))
    builder1.addContent(contextb.build())
    contextb.clearData()
    contextb.setTimestamp(1000000003)
    contextb.setData(ByteString.copyFromUtf8("c3"))
    builder1.addContent(contextb.build())
    contextb.clearData()


    builder1.build().toByteArray
  }

  @Test
  def protobuffer(): Unit = {
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.dataformat.protobuf.ProtobufFactory
    val mapper = new ObjectMapper(new ProtobufFactory) with ScalaObjectMapper
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.PUBLIC_ONLY)
    // mapper.registerModule(DefaultScalaModule).setSerializationInclusion(Include.NON_NULL)
    mapper.registerModule(DefaultScalaModule).setSerializationInclusion(Include.NON_ABSENT)
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)



    val protobuf_str =
      """message CrawlData {
        |        //数据来源：app端固定值为"app"
        |        required string dataSource = 1;
        |        //爬虫版本
        |        required string spiderVersion = 2;
        |        //实例的containerId
        |        optional string containerId = 3;
        |        //实例的cloudServiceId
        |        optional string cloudServiceId = 4;
        |        //用于Saver查找产生该条数据的deepLink
        |        optional string resourceKey = 5;
        |        //数据类型： 1为抓取的业务数据  2为dump的页面数据
        |        required int32 dataType = 6;
        |        //应用的包名
        |        optional string appPackageName = 76;
        |        //应用的版本号
        |        optional string appVersion = 8;
        |        //标识实际抓取的数据的具体类型，由app确定
        |        required string schema = 9;
        |
        |        //实际数据
        |        message Content {
        |            //抓取时间
        |            required int64 timestamp = 1;
        |            //抓取的业务数据为json格式,  dump数据为xml，所有数据Gzip压缩处理，Base64编码
        |            required bytes data = 2;
        |        }
        |        //支持批量传输
        |        repeated Content content = 10;
        |    }""".stripMargin

    import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchemaLoader
    val schema = ProtobufSchemaLoader.std.parse(protobuf_str)


    val bytes=dynamic()
    println(bytes.mkString(","))
   // println(new String(bytes,"ISO-8859-1"))
    println(new String(bytes,"ISO-8859-1").getBytes("ISO-8859-1").mkString(","))

 println(  mapper.readerFor(classOf[Map[String, Any]]).`with`(schema).readValue[Map[String, Any]](new String(bytes,"ISO-8859-1").getBytes("ISO-8859-1")))
  //println(  mapper.readerFor(classOf[Map[String, Any]]).`with`(schema).readValue[Map[String, Any]](Base64.getDecoder.decode(byteString)))
  }


  @Test
def base64(): Unit ={


    val out = new ByteArrayOutputStream
    val in = new ByteArrayInputStream("SDRzSUFBQUFBQUFBQVBNd0tmWjBoQUh2a3ZDUVNsTnZIMS9MaXVEZ0NxOHduNVJndjZyQVNyOHNWOE5RTXdzM1IxK25naUpqazNDUVVnQ3dZeWtRT0FBQUFBPT0=".getBytes)
    val gunzip = new GZIPInputStream(in)
    val buffer = new Array[Byte](1024)
    var n = 0
    while ( {
      n = gunzip.read(buffer)
      n >= 0
    }) out.write(buffer, 0, n)


    gunzip.close()


    println( new String(out.toByteArray))

  }
}
