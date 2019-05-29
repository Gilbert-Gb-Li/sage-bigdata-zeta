package com.haima.sage.bigdata.etl.writer

import com.haima.sage.bigdata.etl.common.model.writer.Xml
import org.junit.Test

/**
  * Created by zhhuiyan on 2016/10/20.
  */
class MapToXMLTest {
  @Test
  def test(): Unit ={
   println(Formatter(Some(Xml())).string(Map("a"->"1","b"->"2")))
  }
}
