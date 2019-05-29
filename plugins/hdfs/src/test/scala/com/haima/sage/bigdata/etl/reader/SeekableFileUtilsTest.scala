package com.haima.sage.bigdata.etl.reader

import java.io.{BufferedReader, FileInputStream, InputStreamReader, RandomAccessFile}

import com.haima.sage.bigdata.etl.monitor.file.SeekableFileUtils
import org.junit.Test

/**
  * Created by lenovo on 2017/10/25.
  */
class SeekableFileUtilsTest {

  @Test
  def resetTest(): Unit ={
    val path = "e:\\intf26.csv"
    val file = new RandomAccessFile(path, "r")
    val inputStream = new FileInputStream(file.getFD)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    if(reader.markSupported()){
      reader.mark(1024 * 1024 * 8)
    }
    val str = reader.readLine()

    SeekableFileUtils.reset(reader)
    val strReset = reader.readLine()
    assert(str.equals(strReset),"")
  }

  @Test
  def lineSeparatorTest(): Unit ={
    val path = "e:\\intf26.csv"
    val file = new RandomAccessFile(path, "r")
    val inputStream = new FileInputStream(file.getFD)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    var read1 = reader.readLine()
    var str = SeekableFileUtils.lineSeparator(reader)
    SeekableFileUtils.reset(reader)
    var read2 = reader.readLine()
    str = SeekableFileUtils.lineSeparator(reader)
    SeekableFileUtils.reset(reader)
    var read3 = reader.readLine()
    println(read1)
    println(read2)
    println(read3)
  }


}