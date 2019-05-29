package com.haima.sage.bigdata.etl.utils

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import org.junit.Test

/**
  * Created by zhhuiyan on 15/3/11.
  */
class DateFormat {

  @Test
  def test(): Unit = {
    val format = new SimpleDateFormat("MMM dd HH:mm:ss ", Locale.ENGLISH)
    println(format.format(new Date()))
    println(format.parse("Mar 11 09:38:16 ").toString)
  }

  @Test
  def testNumberFormat(): Unit = {
    val dt = LocalDateTime.parse("27::Apr::2014 21::39::48",
      DateTimeFormatter.ofPattern("dd::MMM::yyyy HH::mm::ss", Locale.ENGLISH))
    System.out.println("Default format after parsing = " + dt)
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("MMM dd HH:mm:ss", Locale.ENGLISH)
    val format = new SimpleDateFormat("MMM dd HH:mm:ss", Locale.ENGLISH)
    println(format.format(new Date()))
    println(format.parse("May 12 11:16:05").toString)
    /*
        println(formatter.parse("May 12 11:16:05"))
        println(formatter.parse("May 12 11:16:05").toString)
        println(formatter.parse("May 12 11:16:05").toString)*/
  }

}
