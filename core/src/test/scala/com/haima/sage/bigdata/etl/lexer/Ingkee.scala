package com.haima.sage.bigdata.etl.lexer

import com.haima.sage.bigdata.etl.common.model.RichMap
import org.junit.Test

class Ingkee {

  @Test
  def testAdd(): Unit = {
    val event = RichMap(Map("appPackageName" -> "ingkee", "user_id" -> 1, "gift_id" -> "g1", "gift_name" -> "gm"))

    assert(parseAdd(event).size == 7)
  }

  @Test
  def testParse(): Unit = {
    val event = RichMap(Map(
      "dataSource" -> "ds",
      "record_time" -> System.currentTimeMillis(),
      "trace_id" -> "",
      "schema" -> "",
      "timestamp" -> "",
      "cloudServiceId" -> "",
      "spiderVersion" -> "",
      "appVersion" -> "",
      "containerId" -> "",
      "resourceKey" -> "",
      "dataType" -> "-1",
      "data_generate_time" -> "-1",
      "appPackageName" -> "ingkee",
      "room_id" -> "",
      "user_id" -> "u1",
      "audience_id" -> "",
      "audience_name" -> "",
      "gift_id" -> "g1",
      "gift_type" -> "",
      "gift_name" -> "gm",
      "gift_image_url" -> "",
      "gift_num" -> "2",
      "content" -> "",
      "gift_unit_price" -> "-1",
      "type" -> "",
      "gift_type_id" -> "",
      "gift_unit_val" -> 11.0
    ))
    val data = parse(event)("gift_val")
    println(s"$data,${data.getClass}")
    assert(data == 22.0)
  }

  def parseAdd(event: RichMap): RichMap = {
    val appPackageName = event.get("appPackageName").orNull
    val user_id = event.get("user_id").orNull
    val gift_id = event.get("gift_id").orNull
    val gift_name = event.get("gift_name").orNull
    RichMap(event + ("key_user" -> (appPackageName + "-" + user_id + "-" + gift_id)
      , "key_app_id" -> (appPackageName + "-" + gift_id),
      "key_app_name" -> (appPackageName + "-" + gift_name)))
  }


  def parse(event: RichMap): RichMap = {
    import java.text.SimpleDateFormat
    import java.util.Date

    import com.haima.sage.bigdata.etl.common.model.RichMap

    var result = RichMap()
    result ++= event

    //补全数据
    val fields_map = Map(
      "dataSource" -> "",
      "record_time" -> "-1",
      "trace_id" -> "",
      "schema" -> "",
      "timestamp" -> "",
      "cloudServiceId" -> "",
      "spiderVersion" -> "",
      "appVersion" -> "",
      "containerId" -> "",
      "resourceKey" -> "",
      "dataType" -> "-1",
      "data_generate_time" -> "-1",
      "appPackageName" -> "",
      "room_id" -> "",
      "user_id" -> "",
      "audience_id" -> "",
      "audience_name" -> "",
      "gift_id" -> "",
      "gift_type" -> "",
      "gift_name" -> "",
      "gift_image_url" -> "",
      "gift_num" -> "-1",
      "content" -> "",
      "gift_unit_price" -> "-1",
      "type" -> "",
      "gift_type_id" -> "",
      "gift_unit_val" -> "-1",
      "gift_val" -> "-1",
      "gift_unit_val" -> 12
    )
    fields_map.keys.foreach { field =>
      val value = result.get(field).orNull
      if (value == null) {
        val timestamp = System.currentTimeMillis()
        if ("record_time".equals(field) || "trace_id".equals(field)) {
          result ++= Map("record_time" -> timestamp)
          result ++= Map("trace_id" -> timestamp)
        } else if ("data_generate_time".equals(field)) {
          result ++= Map[String, Any]("data_generate_time" -> (result.get("timestamp") match {
            case Some(t: Date) =>
              t.getTime
            case Some(t: String) =>
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t).getTime
            case Some(t: Long) =>
              t
            case _ => timestamp
          })
          )
        } else {
          result ++= Map(field -> fields_map.get(field).orNull)
        }
      } else if ("null".equals(value) || "NULL".equals(value)) {
        //如果数据是null择则获取默认值
        result ++= Map(field -> fields_map.get(field).orNull)
      }
    }

    val gift_type = result.get("gift_type").orNull
    val gift_id = result.get("gift_id").orNull.toString
    result ++= Map("gift_type_id" -> (gift_type + gift_id))

    //补全观众id和系统信息, 如果没有观众id则取观众姓名，若果都没有则认为是系统信息
    val audience_id = result.get("audience_id").orNull
    val audience_name = result.get("audience_name").orNull
    if ("".equals(audience_id) && !"".equals(audience_name)) {
      result ++= Map("audience_id" -> audience_name)
    } else if ("".equals(audience_id) && "".equals(audience_name)) {
      result ++= Map("audience_id" -> "@system_info")
    }

    //知识库补充
    //礼物数量
    val gift_num = result.getOrElse("gift_num", "-1").toString.toInt
    val gift_unit_val = result.getOrElse("gift_unit_val", "-1").toString.toDouble
    if (gift_num > 0) {
      result ++= Map("gift_val" -> gift_num * gift_unit_val)
    }

    //删除字段中的前后空格，以及替换数据中的 制表符（tab) 为字符串"\t"
    fields_map.keys.foreach { field =>
      result.get(field) match {
        case Some(d: String) if d != null =>
          result ++= Map(field -> d.trim.replace("\t", ""))
        case _ =>
      }

    }

    //删除不需要的字段
    val delete_fields: List[String] = List("c@collector", "c@path", "c@receive_time")
    for (key <- delete_fields) {
      result = result - key
    }

    //补hdfs存储目录
    val schema = result.get("schema").orNull
    val data_generate_time = result.get("data_generate_time").orNull
    val time_format = new SimpleDateFormat("yyyy-MM-dd/HH").format(new Date(data_generate_time.toString.toLong))
    val es_format = new SimpleDateFormat("yyyyMMdd").format(new Date(data_generate_time.toString.toLong))
    val hdfsPath = "/data/ias_p3/origin/" + schema + "/" +time_format+ "/data"
    result ++= Map("@hdfsPath" -> hdfsPath,"@es_path"->(schema+"_"+es_format))
    result
  }

  @Test
  def split(): Unit ={
    val event=RichMap(Map("schema"->"live_a_b_c"))
    val schema = event.get("schema").orNull
    println(schema.toString.split("_",2)(1))
  }

}
