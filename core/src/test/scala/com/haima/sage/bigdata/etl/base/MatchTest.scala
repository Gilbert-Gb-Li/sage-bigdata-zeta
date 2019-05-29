package com.haima.sage.bigdata.etl.base

import com.haima.sage.bigdata.etl.normalization.ListType
import org.junit.Test

/**
  * Created by zhhuiyan on 16/8/22.
  */
class MatchTest {
  @Test
def nullTest(): Unit ={
    val a:String =null
    a match {
      case s:String=>
        assert(true)
      case _=>
        assert(assertion = false,"null is not string in match")
    }
  }

  @Test
  def listTest(): Unit ={
     "list[object]" match {
       case ListType(data)=>
         assert(true)
       case _=>
         assert(assertion = false,"list[object] is not ListType")
     }
  }

}
