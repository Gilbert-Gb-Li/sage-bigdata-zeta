package com.haima.sage.bigdata.etl.reader

import java.io.{BufferedReader, File, FileReader, FileWriter}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.codec.{LineCodec, MultiCodec}
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{FileSource, FileWriter, ReadPosition, WriteWrapper}
import com.haima.sage.bigdata.etl.driver.FileMate
import com.haima.sage.bigdata.etl.driver.usable.FileUsabilityChecker
import com.haima.sage.bigdata.etl.monitor.file.LocalFileWrapper
import com.haima.sage.bigdata.etl.utils.Mapper
import com.haima.sage.bigdata.etl.writer.FileDataWriter
import org.junit.Test

/**
  * Created by zhhuiyan on 2017/3/17.
  */
class FileTest extends Mapper{

  import scala.collection.JavaConversions._
  val path = "e:\\filetest.txt"
  val array:Array[String]=Array("2017-06-22 17:42:00,intf6,10.1.96.70,10.193.9.103,14,14,0.085913,00,1,None,0,0,0,0,0,0",
  "2017-06-22 17:42:00,intf6,10.1.96.70,10.193.9.103,14,14,0.085913,00,2,None,0,0,0,0,0,0",
    "2017-06-22 17:42:00,intf6,10.1.96.70,10.193.9.103,14,14,0.085913,00,3,None,0,0,0,0,0,0",
    "2017-06-22 17:42:00,intf6,10.1.96.70,10.193.9.103,14,14,0.085913,00,4,None,0,0,0,0,0,0",
    "2017-06-22 17:42:00,intf6,10.1.96.70,10.193.9.103,14,14,0.085913,00,5,None,0,0,0,0,0,0",
    "2017-06-22 17:42:00,intf6,10.1.96.70,10.193.9.103,14,14,0.085913,00,6,None,0,0,0,0,0,0")

  @Test
  /**
    *读取本地文件，并验证读取数量是否一致
    */
  def fileReader(): Unit ={
//    val path = Paths.get(new URI("e:\\filetest.txt"))
    val file :File = new File("e:\\filetest.txt")
    val fw :java.io.FileWriter = new java.io.FileWriter(file)
    array.foreach(str=>{
      fw.write(str+"\r\n")
    })
    fw.flush()
    fw.close()

    val wrapper = LocalFileWrapper(Paths.get(path),None)
    val reader = new TXTFileLogReader("e:\\filetest.txt",None,None,wrapper,ReadPosition(path,0,0))
    assert(reader.iterator.size==array.length,"读取成功！")

  }

  @Test
  def fileWriter(): Unit ={
//    val wrapper = WriteWrapper(Option("123"), "asdasd", None, FileWriter("", path, None, None, 1000), Option(new Date()), Option(new Date()))
//
//    mapper.writeValueAsString(wrapper)
//    println(mapper.writeValueAsString(WriteWrapper(Option("1"), "asdasd", None, FileWriter("", "/122/1213", None, None, 1000), Option(new Date()), Option(new Date()), 0)))
//    val writer = new FileDataWriter()
  }
  @Test
  def fileChecker(): Unit ={
    val source=Some(FileSource("data/av_log/SystemOut.log", Option("SystemOut"),codec = None))
    val checker = new FileUsabilityChecker(source.get)
//      val info = source.get.info
//      val contentType = source.get.contentType
//      val category = source.get.category
//      val codec  = source.get.codec
    println(checker.check)
  }
  @Test
  def wcByBine(): Unit = {

    val buffer = new BufferedReader(new FileReader(new File("data/json-array-out.json")),1024*1024)
    val set = buffer.lines().iterator().map(line => (line.length, line)).toList.groupBy(_._1).map(dd => (dd._1, (dd._2.size, dd._2.head._2)))
   // set.foreach(println)

    assert(set.size == 1)
    buffer.close()
  }

  @Test
  def attributes(): Unit = {

    val path = Paths.get("data/intf1_20170722174201.csv")
    println(Files.getOwner(path))
    println(Files.readAttributes(path, "*"))


  }

  /*@Test*/
  def create1000File(): Unit = {
    (1 until 1000).foreach {
      f =>

        (1 until 1000).foreach {
          i => {
            (1 to 100).foreach {
              j =>
                val old = if (f == 1) {
                  1000
                } else {
                  f - 1
                }
                val dir = new File(s"data//csv/csv$j")
                if (!dir.exists()) {
                  dir.mkdir()
                }
                val file_o = new File(s"data//csv/csv$j/test-$i-$old.csv")
                file_o.deleteOnExit()

                val file = new File(s"data//csv/csv$j/test-$i-$f.csv")
                val out = file.outputStream()
                out.write(
                  s"""timestamp,index,name,data""".stripMargin.getBytes)
                out.write("\n".getBytes)
                (0 to 99).foreach(index => {
                  out.write(
                    s"""${new Date()},$index,${file.getAbsolutePath},${UUID.randomUUID().toString}""".stripMargin.getBytes)
                  out.write("\n".getBytes)
                })
                out.flush()
                out.close()


            }
            println(s"create:csv-$i-$f")
            if (f != 1) {
              TimeUnit.MINUTES.sleep(1)
            }
          }
        }

    }


  }
}
