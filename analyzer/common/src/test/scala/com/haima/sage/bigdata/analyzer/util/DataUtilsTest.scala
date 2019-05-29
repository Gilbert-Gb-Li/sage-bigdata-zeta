package com.haima.sage.bigdata.analyzer.util

import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.Test
import org.apache.flink.api.scala._

/**
  * Created by lenovo on 2017/11/15.
  */
class DataUtilsTest {

  @Test
  def test(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val list = List(Map("a"->1.0,"b"->2.0,"c"->3.0),
      Map("a"->1.0,"b"->2.0,"d"->3.0),
      Map("a"->1.0,"b"->2.0,"f"->3.0),
      Map("a"->1.0,"d"->2.0,"c"->3.0),
      Map("a"->1.0,"b"->2.0,"c"->3.0))
    val ds = env.fromCollection(list)
    val data = DataUtils.DataPreprocessing(ds,"avg")
    data.print()
  }
}
