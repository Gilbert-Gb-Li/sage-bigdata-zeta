package com.haima.sage.bigdata.etl.utils


import java.io.{BufferedReader, InputStreamReader}

import org.junit.Test

/**
  * Created by zhhuiyan on 15/5/18.
  */
class RuntimeTest {
  @Test
  def exec(): Unit = {


    val stream = sys.runtime.exec("ls ../../").getInputStream

    val br = new BufferedReader(new InputStreamReader(stream))
    val sb = new StringBuilder()
    var line = br.readLine()
    while (line != null) {
      println(line)
      line = br.readLine()
    }
  }
}
