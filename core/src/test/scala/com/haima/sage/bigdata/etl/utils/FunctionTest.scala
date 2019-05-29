package com.haima.sage.bigdata.etl.utils

import org.junit.Test

/**
 * Created by zhhuiyan on 15/5/7.
 */
class FunctionTest {
  @Test
def flatmapTest(): Unit ={
  println( Map("a"->Map("a1"->11,"a2"->12),"b"->Map("b1"->21,"a2"->22),"c"->Map("c1"->21,"c2"->22))
      .flatMap(tuple=>tuple._2))
  }

  @Test
  def listReduce(): Unit ={
    val data=  List.empty[Map[String,String]]
    data match {
      case Nil=>
      case d=>
        d.reduce((f,s)=>f++s)
    }

  }
}
