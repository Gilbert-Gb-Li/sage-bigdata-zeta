package com.haima.sage.bigdata.etl.stream

import java.nio.file.Paths

import com.haima.sage.bigdata.etl.common.model.ExcelFileType
import com.haima.sage.bigdata.etl.monitor.file.LocalFileWrapper
import org.junit.Test

class ExcelStreamTest {
  @Test
  def readFile(): Unit = {

    val stream = ExcelStream(
      LocalFileWrapper(Paths.get(this.getClass.getResource("/excel.xlsx").toURI)),
      Some(ExcelFileType("")))
    val its = stream.take(10)
    assert(its.size == 9)
  }
}
