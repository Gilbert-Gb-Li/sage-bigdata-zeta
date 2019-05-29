package com.haima.sage.bigdata.etl.writer

import java.io.File
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.{FileWriter, WriteWrapper}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test
import com.haima.sage.bigdata.etl.common.Implicits._

/**
  * Created by zhhuiyan on 2017/3/14.
  */
class FileWriterTest extends Mapper {


  @Test
  def write(): Unit = {

    println(mapper.writeValueAsString(FileWriter("", "/122/1213", None, None, 1000)))

    val w = new File("data/json-array.out.json").bufferedWriter()
    w.newLine()
    w.write("""{"a":1}""")
    w.flush()
    w.close()
  }


  @Test
  def writeT(): Unit = {


    println(mapper.writeValueAsString(Some(FileWriter("", "/122/1213", None, None, 1000))))
    println(mapper.writeValueAsString(WriteWrapper(Option("1"), "asdasd", None, FileWriter("", "/122/1213", None, None, 1000), Option(new Date()), Option(new Date()), 0)))

  }
}