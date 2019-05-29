package com.haima.sage.bigdata.etl.writer

import com.haima.sage.bigdata.etl.common.model.{FileWriter, HDFSWriter}
import com.haima.sage.bigdata.etl.driver.HDFSDriver
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.Test

import scala.util.{Failure, Success}

class HDFSWriterTest {

  val NAMESERVICES = "HaimaDEV"
  val NAMENODES = "nn1,nn2"
  val HOSTS = "172.16.208.67:8020,172.16.208.68:8020"
  val URI = "/data/tmp/test.txt"

  val conf = HDFSWriter(
    "123",
    HOSTS,
    NAMESERVICES,
    NAMENODES,
    FileWriter("234", URI),
    None
  )
  val fs: FileSystem = HDFSDriver(conf).driver() match {
    case Success(f) =>
      f
    case Failure(e) =>
      throw e
  }

  @Test
  def testCreate(): Unit ={
    val out = fs.create(new Path(URI))
    out.write("HDFS write data test".getBytes)
    out.flush()
    out.close()
  }

  @Test
  def testAppend(): Unit ={
    val out = fs.create(new Path(URI))
    out.write("HDFS write data append test".getBytes)
    out.flush()
    out.close()
  }


}
