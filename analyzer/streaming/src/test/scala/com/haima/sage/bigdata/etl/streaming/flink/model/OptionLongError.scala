package com.haima.sage.bigdata.etl.streaming.flink.model

import org.junit.Test

class OptionLongError {

  @Test
  def optcast(): Unit = {
    val bfa:Int=1

    val a:Option[Int] = Some(bfa)

    val b: Long = a.get
    assert(b.getClass != classOf[java.lang.Integer])

  }

}
