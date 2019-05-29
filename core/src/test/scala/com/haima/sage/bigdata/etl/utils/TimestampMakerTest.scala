package com.haima.sage.bigdata.etl.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.etl.normalization.format.LocalDateTranslator
import org.junit.Test

import scala.collection.immutable.HashMap

/**
  * Created by zhhuiyan on 15/2/26.
  */
class TimestampMakerTest {
  @Test
  def get(): Unit = {
    val FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss.S Z'Z'")
    println(FORMAT.format(new Date(1476264493000l)))
  }

  @Test
  def testdata(): Unit = {


    var log: Map[String, Any] = Map("@timestamp" -> "Aug 24 2015 23:59:53")

    //  System.out.println("log = " + TimestampMaker().suffix(log))
  }

  @Test
  def main() {
    var log: Map[String, Any] = new HashMap[String, AnyRef]
    /* System.out.println("new Date().getTime() = " + new Date)
     System.out.println("new Date().getTime() = " + new Date().getTime)
     log += (("@timestamp", "1410225299.774338"))
     System.out.println("log = " + TimestampMaker().suffix(log))
     System.out.println("log = " + log.get("@timestamp"))*/

    val FORMAT = new SimpleDateFormat(
      "yyyyMMdd")
    log += (("@timestamp", 1.450567914232E12))
   // System.out.println("log => " + FORMAT.format(TimestampMaker().suffix(log)))
    System.out.println("log => " + log.get("@timestamp"))
  }

  @Test
  def testIIS(): Unit = {
    var log: Map[String, Any] = new HashMap[String, AnyRef]
    log += (("date", "20150623"))
    log += (("time", "05:52:59"))
    //log += (("@timestamp", "2013-05-04T16:15:36:176Z"))
    //System.out.println("log => " + TimestampMaker().suffix(log))
  }

  @Test
  def testfomart(): Unit = {
    val fort = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss Z")
    println(fort.parse("2016-03-21 12:05:08 +0000"))
    val fort2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    println(fort2.parse("2016-03-21 12:05:08"))
  }

  @Test
  def testAAA(): Unit = {
    val FORMAT_F = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z")
    val FORMAT = new SimpleDateFormat("yyyyMMdd")
    val value = FORMAT.format(FORMAT_F.parse("2016-08-10 06:25:17 +0000"))
    val first = FORMAT.parse(value).getTime
    val rps = "\\d+".r.findFirstIn(value) match {
      case Some(v) =>
        (v.toInt + 1).toString
      case _ =>
        "0"
    }
    val second = FORMAT.parse(value.replaceFirst("\\d+", rps)).getTime
    println((FORMAT_F.format(new Date(first)), FORMAT_F.format(new Date(second))))
  }

  @Test
  def testTo(): Unit = {

    val FORMAT_F = LocalDateTranslator("yyyy-MM-dd HH:mm:ss Z")
    println(FORMAT_F.parse("2016-08-10 06:25:17 +0000"))
  }


}
