package com.haima.sage.bigdata.etl.base

import org.junit.Test

/**
  * Created by zhhuiyan on 16/9/6.
  */
class EncodingLengthTest {
  @Test
  def test(): Unit ={

    println(s"lenth:${"中国人".length},UTF-8:${"中国人".getBytes("UTF-8").length}")
    println(s"lenth:${"中国人".length},GBK:${"中国人".getBytes("GBK").length}")
    println(s"lenth:${"中国人".length},ISO8859-1:${"中国人".getBytes("ISO8859-1").length}")
    println(s"lenth:${"中国人".length},GB2312:${"中国人".getBytes("GB2312").length}")
    /*assert("中国人".length=="中国人".getBytes("UTF-8").length,"UTF-8 must be eq ")
    assert("中国人".length=="中国人".getBytes("GBK").length,"GBK must be eq --")
    assert("中国人".length=="中国人".getBytes("ISO8859-1").length,"ISO8859-1 must be eq --")
    assert("中国人".length=="中国人".getBytes("GB2312").length,"GB2312 must be eq --")*/
  }

}
