package com.haima.sage.bigdata.etl.lexer.filter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Date
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.haima.sage.bigdata.etl.common.model.filter.{MapCase, _}
import com.haima.sage.bigdata.etl.common.model.{JsonParser, RichMap}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

/**
  * Created by zhhuiyan on 16/1/26.
  */
class FilterTest extends Mapper {

  @Test
  def zipTest(): Unit ={
    val data="123123232313124324242312313"
    val out = new ByteArrayOutputStream()
    val gzip = new GZIPOutputStream(out)
    gzip.write(data.getBytes())
    gzip.close()
    val ziped=new String(out.toByteArray,"ISO-8859-1")
   println(ziped)

    val out2 = new ByteArrayOutputStream
    val in = new ByteArrayInputStream(ziped.getBytes("ISO-8859-1"))
    val gunzip = new GZIPInputStream(in)
    val buffer = new Array[Byte](1024)
    var n = 0
    while ( {
      n = gunzip.read(buffer)
      n >= 0
    }) out2.write(buffer, 0, n)
    gunzip.close()
    println(new String(out2.toByteArray))
  }


  import com.haima.sage.bigdata.etl.common.Implicits._

  def execute(data: RichMap)(implicit rule: MapRule): (String, RichMap) = {
    val filter = Filter(Array(rule))
    (s"data[$data] \n after filter[ ${mapper.writeValueAsString(rule)}]", filter.filter(data).head)
  }

  def execute(data: List[RichMap])(implicit rule: MapRule): (String, List[RichMap]) = {
    val filter = Filter(Array(rule))
    (s"data[$data] \n after filter[ ${mapper.writeValueAsString(rule)}]", data.map(filter.filter).head)
  }

  @Test
  def scriptFilterTest(): Unit = {
    val add1: AddFields = AddFields(Map("@timestamp" -> "1"))
    val add2: AddFields = AddFields(Map("@timestamp" -> "2"))
    implicit val rule = MapScriptFilter("event.get('a')","js", Array(MapCase("1", add1)), Some(add2))
    val data = Map("a" -> "1")
    val (name, filtered) = execute(data)

    println(filtered)
    assert(filtered.get("@timestamp").contains("1"), s" $name after script must be 1")
  }

  @Test
  def addFieldsTest1(): Unit = {
    val data = Map("alarmTime" -> "121312312")
    implicit val rule: AddFields = AddFields(Map("@timestamp" -> "%{alarmTime}"))
    val (name, filtered) = execute(data)
    assert(filtered.contains("@timestamp"), s" $name must be contain key @timestamp")
    assert(filtered.get("@timestamp").contains("121312312"), s"$name must be contain key @timestamp with value[121312312]")
    assert(filtered.contains("alarmTime"), s"$name must also contain key alarmTime")
  }

  @Test
  def addFieldsTest2(): Unit = {
    val data = Map("alarmTime" -> Map("day" -> "12"))
    implicit val rule: AddFields = AddFields(Map("@timestamp" -> "%{alarmTime.day}"))
    val (name, filtered) = execute(data)
    assert(filtered.contains("@timestamp"), s" $name must be contain key @timestamp")
    assert(filtered.get("@timestamp").contains("12"), s"$name must be contain key @timestamp with value[12]")
    assert(filtered.contains("alarmTime.day"), s"$name must also contain key alarmTime.day")
  }

  @Test
  def addFieldsTest3(): Unit = {
    val data = Map("alarmTime" -> Map("day" -> "12"))
    implicit val rule: AddFields = AddFields(Map("time.day" -> "%{alarmTime.day}"))
    val (name, filtered) = execute(data)
    assert(filtered.contains("time.day"), s" $name must be contain key time.day")
    assert(filtered.get("time.day").contains("12"), s"$name must be contain key time.day with value[12]")
    assert(filtered.contains("alarmTime.day"), s"$name must also contain key alarmTime.day")
  }

  @Test
  def reParserTest(): Unit = {
    val data = Map("raw" -> Map("date" -> """{"year":"2017","mouth":"11","day":"24"}"""))
    implicit val rule: ReParser = ReParser(Some("raw.date"), JsonParser(Array(), None), None)
    val (name, filtered) = execute(data)
    assert(filtered.contains("year"), s" $name must be contain key year")
    assert(filtered.contains("mouth"), s" $name must be contain key mouth")
    assert(filtered.contains("day"), s" $name must be contain key day")
  }

  @Test
  def fieldCutTest(): Unit = {
    val data = Map("raw" -> Map("date" -> """"2017-11-24""""))
    implicit val rule: FieldCut = FieldCut("raw.date", 1, 10)
    val (name, filtered) = execute(data)

    assert(filtered.get("raw.date").isDefined, s" $name must be contain key year")
    assert(filtered.get("raw.date").contains("2017-11-24"), s"$name must be contain key raw.date with value[2017-11-24]")
  }

  @Test
  def mappingTest(): Unit = {
    val data = Map("alarmTime" -> "121312312")
    val rule: Mapping = Mapping(Map("alarmTime" -> "@timestamp"))
    val (name, filtered) = execute(data)(rule)
    assert(filtered.contains("@timestamp"), s"$name must be contain key @timestamp")
    assert(!filtered.contains("alarmTime"), s"$name must not contain key @timestamp")
    /*
    * t_cm_endpoint -> {
    endpoint=10.10.0.12_NULL0,
     ,
    display_name=RZ-IDC-I02-MSW-S2720-01_端口_2,
    update_time=Mon Oct 16 16:24:19 CST 2017},
    t_cm_category -> {
    category_code=900002001,
    category_name=端口}*/
    val data2 = Map(
      "t_cm_endpoint" -> Map("endpoint" -> "10.10.0.12_NULL0",
        "endpoint_name" -> "RZ-IDC-I02-MSW-S2720-01_端口_2",
        "display_name" -> "RZ-IDC-I02-MSW-S2720-01_端口_2",
        "update_time" -> new Date()),
      "t_cm_category" -> Map("category_code" -> "900002001",
        "category_name" -> "端口"
      )
    )

    implicit val rule2: Mapping = Mapping(Map("t_cm_endpoint.endpoint" -> "endpoint",
      "t_cm_endpoint.endpoint_name" -> "endpoint_name",
      "t_cm_endpoint.display_name" -> "display_name",
      "t_cm_endpoint.update_time" -> "update_time",
      "t_cm_category.category_code" -> "category_code",
      "t_cm_category.category_name" -> "category_name"))
    val (name2, filtered2) = execute(data2)

    assert(filtered2.forall(!_._2.isInstanceOf[Map[_, _]]))
    assert(filtered2.size == 6)


  }

  @Test
  def switchCaseWithContain(): Unit = {
    val data = List[RichMap](Map("path" -> "/opt/test/test/bpc_csv_export/app4/intf5/intf26_2017072219209.csv"),
      Map("path" -> "/opt/test/test/bpc_csv_export/app2/intf5/intf26_2017072219209.csv"),
      Map("path" -> "/opt/test/test/bpc_csv_export/app2/intf5/intf26_2017072219209.csv"),
      Map("path" -> "/opt/test/test/bpc_csv_export/app3/intf5/intf26_20170722192019.csv"),
      Map("path" -> "/opt/test/test/bpc_csv_export/app3/intf2/intf26_2017072219209.csv"))

    implicit val rule: MapContain = MapContain("path", Array(
      MapCase("app4/intf5", AddFields(Map("TYPE" -> "APP4"))),
      MapCase("app2/intf5", AddFields(Map("TYPE" -> "APP2")))
    ), Some(Drop()))
    val (name, filtered) = execute(data.head)
    assert(filtered.get("TYPE").contains("APP4"), s"$name must be contain key @timestamp with value[APP4]")

    val rt2 = data.map(execute).map(_._2)
    assert(rt2.count(_.exists(_._2 == "APP4")) == 1, s"$name must be only one key[TYPE] with value[APP4]")
    assert(rt2.count(_.exists(_._2 == "APP2")) == 2, s"$name  key[TYPE] with value[APP2] size muse be 2  ")
    assert(rt2.count(_.nonEmpty) == 3, s"$name   non empty must be  3   ")

  }
  @Test
  def replaceTest1(){
    implicit  val rule :Replace=Replace("f1","13","%{f2}-15")
    implicit val data=List[RichMap](Map("f1"->"abc-13-2","f2"->"中午"))
    val (name, filtered) = execute(data.head)

    println(filtered)
    assert(filtered.get("f1").contains("abc-中午-15-2"), s"$name must be contain key f1 with value[abc-中午-15-2]")
  }
  @Test
  def replaceTest2(){
    implicit  val rule :Replace=Replace("f1","13","%{f2}-15")
    implicit val data=List[RichMap](Map("f1"->"abc-14-2","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    assert(filtered.get("f1").contains("abc-14-2"), s"$name must be contain key f1 with value[abc-14-2]")
  }
  @Test
  def replaceTest3(){
    implicit  val rule :Replace=Replace("f1","13","%{f3}-15")
    implicit val data=List[RichMap](Map("f1"->"abc-13-2","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    assert(filtered.get("f1").contains("abc--15-2"), s"$name must be contain key f1 with value[abc-14-2]")
  }

  @Test
  def FieldAdditiveTest1(){
    implicit  val rule :FieldAdditive=FieldAdditive("f1",13)
    implicit val data=List[RichMap](Map("f1"->2,"f2"->"中午"))
    val (name, filtered) = execute(data.head)
    println(filtered)
    assert(filtered.get("f1").contains(15), s"$name must be contain key f1 with value[26]")
  }
  @Test
  def FieldAdditiveTest2(){
    implicit  val rule :FieldAdditive=FieldAdditive("f1",13)
    implicit val data=List[RichMap](Map("f1"->"2","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    println(filtered)
    assert(filtered.get("f1").contains(15), s"$name must be contain key f1 with value[15]")
  }
  @Test
  def FieldAdditiveTest3(){
    implicit  val rule :FieldAdditive=FieldAdditive("f1",10)
    implicit val data=List[RichMap](Map("f1"->"2.5","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    println(filtered)
    assert(filtered.get("f1").contains(12.5), s"$name must be contain key f1 with value[12.5]")
  }

  @Test
  def FieldAdditiveTest4(){
    implicit  val rule :FieldAdditive=FieldAdditive("f1",10)
    implicit val data=List[RichMap](Map("f1"->".5","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    println(filtered)
    assert(filtered.get("f1").contains(10.5), s"$name must be contain key f1 with value[10.5]")
  }
  @Test
  def FieldAdditiveTest5(){
    implicit  val rule :FieldAdditive=FieldAdditive("f1",10)
    implicit val data=List[RichMap](Map("f1"->"5.","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    println(filtered)
    assert(filtered.get("f1").contains(15), s"$name must be contain key f1 with value[15]")
  }
  @Test
  def FieldMultiTest1(){
    implicit  val rule :FieldMulti=FieldMulti("f1",2)
    implicit val data=List[RichMap](Map("f1"->"5.","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    println(filtered)
    assert(filtered.get("f1").contains(10), s"$name must be contain key f1 with value[10]")
  }

  @Test
  def FieldMultiTest2(){
    implicit  val rule :FieldMulti=FieldMulti("f1",2)
    implicit val data=List[RichMap](Map("f1"->"-5.","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    println(filtered)
    assert(filtered.get("f1").contains(-10), s"$name must be contain key f1 with value[10]")
  }

  @Test
  def FieldMultiTest3(){
    implicit  val rule :FieldMulti=FieldMulti("f1",-2)
    implicit val data=List[RichMap](Map("f1"->"-5.","f2"->"中午"))
    val (name, filtered) = execute(data.head)
    println(filtered)
    assert(filtered.get("f1").contains(10), s"$name must be contain key f1 with value[10]")
  }

  @Test
  def Extends1(){
    implicit  val rule :Extends=Extends("f1")
    implicit val data=List[RichMap](Map("f1"->Array("a","b","c"),"f2"->"中午"))
    val filter = Filter(Array(rule))
    val filtered =filter.filter(data.head)
    println(filtered.size)
    println(filtered)
    assert(filtered.size==3, s"$Extends must be from 1map to 3map")
  }
  @Test
  def Extends2(){
    implicit  val rule :Extends=Extends("f1")
    implicit val data=List[RichMap](Map("f1"->Array(Map("f3"->"1"),Map("f3"->"2"),Map("f3"->"3")),"f2"->"中午"))
    val filter = Filter(Array(rule))
    val filtered =filter.filter(data.head)
    println(filtered.size)
    println(filtered)
    assert(filtered.size==3, s"$Extends must be from 1map to 3map")
    assert(filtered.head.contains("f3"), s"$Extends must be contains f3 ")
  }




}
