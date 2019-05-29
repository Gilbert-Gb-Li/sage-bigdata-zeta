package com.haima.sage.bigdata.etl.utils

import org.junit.Test

/**
  * Created by zhhuiyan on 15/3/24.
  */
class EncodingTest {
  @Test
  def test(): Unit = {
    val str = new String(new String(
      """15:01:42" fw=滨海公安 pri=6 type=system user=superman src=10.88.134.30 op="logout" result=0 recorder=AUTH msg=""
      """.getBytes(), "GBK").getBytes("GBK"), "utf-8")
    println(str)
  }
}
