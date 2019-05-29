package com.haima.sage.bigdata.etl.base

import org.junit.Test


/**
  * Created by zhhuiyan on 15/5/19.
  */
class EqualsTest {
  @Test
  def testZero(): Unit = {
    val a: List[String] = null
    val b: List[String] = Nil
    assert(b == a)
    assert(0 % 2 == 0)
    assert(1 % 2 == 1)
    assert(3 % 2 == 1)
    assert(4 % 2 == 0)
  }

  @Test
  def hash(): Unit = {
    println("nacUser_UserLoginLog".hashCode)
  }

  @Test
  def list(): Unit = {
    val alist = List("1", "2", "3").sorted
    assert(alist.equals(List("1", "3", "2").sorted))
  }


}


