package com.haima.sage.bigdata.etl.utils

import org.junit.Test

/**
  * Created by zhhuiyan on 15/12/29.
  */
class IPLongConvertTest {
  @Test
  def to(): Unit ={
   assert(IPLongConvert.to("192.168.16.1")==3232239617l)
  }
  @Test
  def from(): Unit ={
    assert(IPLongConvert.from(3232239617l)=="192.168.16.1")
  }
}
