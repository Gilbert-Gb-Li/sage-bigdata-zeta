package com.haima.sage.bigdata.etl.utils

import java.security.MessageDigest

import org.junit.Test

/**
  * Created by zhhuiyan on 16/6/16.
  */
class IDMakeTester {
  val hexDigits = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
  val mdInst = MessageDigest.getInstance("MD5")

  @Test
  def make(): Unit = {
    val m = Map[String, Any]("a" -> null, "c" -> 12)
    println(m.get("a"))
    println(m.get("b"))
    println(m.get("c"))
    val data = Array("a", "b", "c").map(id => m.get(id) match {
      case Some(null) =>
        null
      case Some(d) =>
        d.toString
      case _ =>
        null
    }).filter(_ != null).mkString("-")
    println(data)
    val md = mdInst.digest(data.getBytes)
    // 把密文转换成十六进制的字符串形式
    val j = md.length
    val str = (0 until j).map { i =>
      List(hexDigits(md(i) >>> 4 & 0xf), hexDigits(md(i) & 0xf))

    }.reduce[List[Char]] {
      case (a, b) =>
        b ::: a
    }.toArray
    println(new String(str))

  }

}
