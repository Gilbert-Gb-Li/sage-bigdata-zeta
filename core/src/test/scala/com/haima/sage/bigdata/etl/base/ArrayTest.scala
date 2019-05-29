package com.haima.sage.bigdata.etl.base

import org.junit.Test

class ArrayTest {
  @Test
  def nonEmpty(): Unit = {
    val a="12"
    val b=Array(1)

    assert( a match {
      case "12" if b.nonEmpty=>
        true
      case _=>
        false
    },"array nonEmpty must be true in case")
    assert(Array(1).nonEmpty,"array nonEmpty must be true")
  }
}
