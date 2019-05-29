package com.haima.sage.bigdata.etl.writer

import java.util.Date

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.writer._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

/**
  * Created by zhhuiyan on 2016/11/15.
  */
class NameFormatterTest extends Mapper {
  @Test
  def date(): Unit = {
    val ref = RefDate("datefield", "yyyyMMdd")
    val formatter = NameFormatter("logs_$", Some(ref))
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ref))
    val data = Map("datefield" -> new Date())
    println(formatter.format(data))
  }

  @Test
  def ref(): Unit = {
    val ref = Ref("datefield")
    val formatter = NameFormatter("logs_$", Some(ref))
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ref))
    val data = Map("datefield" -> "13")
    println(formatter.format(data))
  }

  @Test
  def refTruncate(): Unit = {
    val ref = RefTruncate("ref", end = true, 0, 5)
    val formatter = NameFormatter("logs_$", Some(ref))
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ref))
    val data = Map("ref" -> 13146724775l)
    println(formatter.format(data))
  }

  @Test
  def refHashMod(): Unit = {
    val ref = RefHashMod("ref", 10)
    val formatter = NameFormatter("logs_$", Some(ref))
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ref))
    val d = 1314
    println(d.hashCode())
    val data = Map("ref" -> 13146724775l)
    println(formatter.format(data))
  }
}
