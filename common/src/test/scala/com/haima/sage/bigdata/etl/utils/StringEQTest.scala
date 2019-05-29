package com.haima.sage.bigdata.etl.utils

import java.io._

import org.junit.Test

/**
  * Created by zhhuiyan on 2017/5/23.
  */
class StringEQTest {

  @Test
  def eq(): Unit = {
    val file = new File("string-qe.file")

    assert(file.getName eq "string-qe.file", "eq is ref equals")
    assert(file.getName == "string-qe.file", "== is deep equals")
    assert(file.getName equals "string-qe.file", "equals is deep object equals")

  }
}
