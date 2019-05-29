package com.haima.sage.bigdata.etl.utils

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import org.junit.Test

/**
  * Created by zhhuiyan on 15/2/28.
  */
class RegexTest {
  @Test
  def splitdot(): Unit = {
    val path = "parent.sub"
    val paths = path.split("\\.")
    println(paths.mkString("."))
  }
  @Test
  def tab_line_space(): Unit = {
      "23123131.[".split("?").foreach(println)
  }

  @Test
  def split(): Unit = {
  /*  val path = "zeta/target/releases/worker/conf/"
    val paths = path.split("/")
    println(paths.mkString("/"))*/
  val path = "1223123_topic_1_5544"
    val uri = path.split("_")
    val topic = uri.slice(1, until = uri.length - 1).mkString("_")
    println(topic, uri.last)
    assert(topic == "topic_1")
    assert(uri.last == "5544")

  }




  @Test
  def testReplace(): Unit = {
    val a = "^\\d".r
    println(a.findFirstIn("3"))
    println(a.findFirstIn(" 3"))


    assert(""""123""".replaceAll("""^['"]?(.*?)['"]?$""", "$1") == "123")
    assert("""'123'""".replaceAll("""^['"]?(.*?)['"]?$""", "$1") == "123")
  }

  @Test
  def testPhone(): Unit = {
    assert("^1(([358]{1}[0-9]{1})|(47|70|76|77|78))[0-9]{8}$".r.pattern.matcher("15146724775").find())
  }

  @Test
  def testRuixing(): Unit = {
    assert("^1(([358]{1}[0-9]{1})|(47|70|76|77|78))[0-9]{8}$".r.pattern.matcher("15146724775").find())
  }

  @Test
  def testNamedGroup(): Unit = {
    val ptn = Pattern.compile("(?<[abc]>abc)")
    val matcher = ptn.matcher("abc")
    matcher.find()
  }

  @Test
  def testFormart(): Unit = {
    val FORMAT = new SimpleDateFormat(
      "yyyyMMdd")

    val value = "20150131"
    val rps = "\\d+".r.findFirstIn(value) match {
      case Some(v) =>
        (v.toInt + 1).toString
      case _ =>
        "0"
    }
    val add1 = value.replaceFirst("\\d+", rps)
    println(add1)
    println(FORMAT.format(FORMAT.parse(add1)))


  }

  @Test
  def addYearFormart(): Unit = {
    val FORMAT = new SimpleDateFormat(
      "yyyyMMdd")

    val value = "20150131"
    val first = FORMAT.parse(value).getTime

    println(first)


  }

  @Test
  def testSplit(): Unit = {
    val data = "id=tos time=\"2015-04-01 16:02:38\" fw=滨海公安IDP-备  pri=6 type=ac  recorder=FW-NAT src=10.88.134.111 dst=224.0.0.22 sport=0 dport=0 \t\t\tsmac=00:0a:f7:1b:a3:81 dmac=01:00:5e:00:00:16 proto=unknow indev=null outdev=null user= rule=accept \t\t\tconnid=512981098 parentid=0 dpiid=0 natid=0 policyid=8066 msg=\"null\"\r\nid=tos time=\"2015-04-01 16:02:38\" fw=滨海公安IDP-备  pri=6 type=conn  src=10.88.134.111 dst=224.0.0.22 proto=unknown proto sport=0 dport=0 \t\t\tinpkt=1 outpkt=0 sent=40 rcvd=0 duration=0 connid=512981098 msg=\"null\"\r\nid=tos time=\"2015-04-01 16:02:38\" fw=滨海公安IDP-备  pri=6 type=ac  recorder=FW-NAT src=169.254.198.216 dst=224.0.0.22 sport=0 dport=0 \t\t\tsmac=00:0a:f7:1b:a3:80 dmac=01:00:5e:00:00:16 proto=unknow indev=null outdev=null user= rule=accept \t\t\tconnid=512981100 parentid=0 dpiid=0 natid=0 policyid=8066 msg=\"null\"\r\nid=tos time=\"2015-04-01 16:02:38\" fw=滨海公安IDP-备  pri=6 type=conn  src=169.254.198.216 dst=224.0.0.22 proto=unknown proto sport=0 dport=0 \t\t\tinpkt=1 outpkt=0 sent=40 rcvd=0 duration=0 con"
    val value = data.split("\r\n")
    println(s"data.length:${data.length},split.len:${value.length}")
    value.foreach(println)
  }

  @Test
  def testMatch(): Unit = {
    val data = "id=tos time=\"2015-04-01 16:02:38\" fw=滨海公安IDP-备  pri=6 type=ac  recorder=FW-NAT src=10.88.134.111 dst=224.0.0.22 sport=0 dport=0 \t\t\tsmac=00:0a:f7:1b:a3:81 dmac=01:00:5e:00:00:16 proto=unknow indev=null outdev=null user= rule=accept \t\t\tconnid=512981098 parentid=0 dpiid=0 natid=0 policyid=8066 msg=\"null\"\r\nid=tos time=\"2015-04-01 16:02:38\" fw=滨海公安IDP-备  pri=6 type=conn  src=10.88.134.111 dst=224.0.0.22 proto=unknown proto sport=0 dport=0 \t\t\tinpkt=1 outpkt=0 sent=40 rcvd=0 duration=0 connid=512981098 msg=\"null\"\r\nid=tos time=\"2015-04-01 16:02:38\" fw=滨海公安IDP-备  pri=6 type=ac  recorder=FW-NAT src=169.254.198.216 dst=224.0.0.22 sport=0 dport=0 \t\t\tsmac=00:0a:f7:1b:a3:80 dmac=01:00:5e:00:00:16 proto=unknow indev=null outdev=null user= rule=accept \t\t\tconnid=512981100 parentid=0 dpiid=0 natid=0 policyid=8066 msg=\"null\"\r\nid=tos time=\"2015-04-01 16:02:38\" fw=滨海公安IDP-备  pri=6 type=conn  src=169.254.198.216 dst=224.0.0.22 proto=unknown proto sport=0 dport=0 \t\t\tinpkt=1 outpkt=0 sent=40 rcvd=0 duration=0 con"
    val value = data.split("\r\n")

    println(s"id=tos".r.findFirstIn(data))
    value.foreach(println)
  }

  @Test
  def testArray(): Unit = {


    val data = Array(("172.16.219.130",9200), ("172.16.219.131",9200)).map(data => data._1 + ":" + data._2).mkString(",")

    println(data)
  }

  @Test
  def matchs(): Unit ={
    assert("/opt/test/test/bpc_csv_export/app3/intf5/intf26_2017072219209.csv".matches(".*app4/intf5.*"))
  }

  @Test
  def testMap(): Unit = {


    val data = Map("a_aaa" -> 10, "b_aaa" -> 11, "c_aaa" -> 12).filterNot(d => d._1 == "a_aaa" || d._1 == "b_aaa")

    println(data)
  }
}

