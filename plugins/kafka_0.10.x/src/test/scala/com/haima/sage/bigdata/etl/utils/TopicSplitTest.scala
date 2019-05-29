package com.haima.sage.bigdata.etl.utils

import org.junit.Test

class TopicSplitTest {
  @Test
  def split(): Unit = {
    val uri = "path_a_b_d_f_d_c_0".split("_").reverse


    if (uri.length > 1) {
      val topic = uri.slice(1, until = uri.length - uri.length / 2).reverse.mkString("_")
      val partition = uri(0).toInt

      println(topic, partition)
    }
  }

}
