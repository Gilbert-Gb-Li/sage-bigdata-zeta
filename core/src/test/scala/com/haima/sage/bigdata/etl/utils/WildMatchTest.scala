package com.haima.sage.bigdata.etl.utils

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import com.haima.sage.bigdata.etl.common.model.ReadPosition
import com.haima.sage.bigdata.etl.monitor.file.LocalFileWrapper
import com.haima.sage.bigdata.etl.reader.TXTFileLogReader
import org.junit.Test

/**
  * Created by zhhuiyan on 15/5/22.
  */
class WildMatchTest {
 @Test
  def line(): Unit ={
   val length= "2017-06-22 17:42:00,intf6,10.1.96.70,10.193.9.103,62,62,0.029999,00,改密,None,0,0,0,0,0,0".length
   val length2= "2017-06-22 17:42:00,intf6,10.1.96.70,10.193.9.103,62,62,0.029999,00,改密,None,0,0,0,0,0,0".length
  println(s"length:$length,$length2")
   assert(length  ==length2)
 }

  @Test
  def path(): Unit = {

    println(System.getProperty("os.name"))
    val _match = new WildMatch {}
    "/var".split("/").
      foreach {
        a =>
          println(a)
      }


    println(nameWithPath(""))
    val pattern = _match.toPattern("/root/?.log".toCharArray);


    assert(pattern.matcher("/root/b.log").matches())
  }

  def nameWithPath(path: String): (String, String) = {
    val split: String = File.separator
    val paths = path.split("[\\\\/]")
    if (paths.isEmpty) {
      ("/", "." + split)
    } else if (paths.length == 1) {
      (path, "." + split)
    } else if (paths.length == 2) {
      paths(0) match {
        case "" =>
          (paths(paths.length - 1), split)
        case p =>
          (paths(paths.length - 1), p)
      }
    } else {
      (paths(paths.length - 1), paths.slice(0, paths.length - 1).mkString("/"))
    }
  }
  @Test
  def local(): Unit = {
    val matcher = new PathWildMatch[LocalFileWrapper]() {


      override protected def loggerName: String = "PathWildMatch"

      override def getFileWrapper(path: String): LocalFileWrapper = LocalFileWrapper(Paths.get(path), None)
    }
    val (key, values) = matcher.parse("data/iislog/SZ_test/*")

    println(matcher.nameWithPath("/"))

    val data = key
    // val stream=  new TXTFileLogReader("/SZ_test"+data.name, None,new BufferedReader( new InputStreamReader(data.stream, "gbk"),1024*1024), new ReadPosition("/SZ_test"+data.name,0,0),1)
    new Runner("1", data).start()
    /* matcher.mkData(key).listFiles().foreach{
       sub =>
         if (!sub.isDirectory) {
           println(sub.name)

           val stream=  new TXTFileLogReader("/SZ_test"+sub.name, None,new BufferedReader( new InputStreamReader(sub.stream, "gbk"),1024*1024), new ReadPosition("/SZ_test"+sub.name,0,0),1)

           new Thread() {
             override def run(): Unit = {
               println()
               print(sub.name+":")
               stream.foreach(println)
               stream.close()
               println(getName + ":" + sub.name + ":closed")
             }
           }.start()

           //fs.completePendingCommand()
         }
     }*/
    val (data2, values2) = matcher.parse("data/iislog/SZ_test/")
    new Runner("2", data2).start()
    /* matcher.mkData(key2).isDirectory

     matcher.mkData(key).exists()
     matcher.mkData(key).absolutePath
     matcher.mkData(key).stream.close()*/
    //matcher.mkData(key).file.exists()
    println(s"root:$key")
    Thread.sleep(10000000)
    // matcher.files(LocalFileWrapper(Paths.get(key+"/")),values).foreach(file=>println(file.getAbsolutePath))
  }
}

class Runner(name: String, data: LocalFileWrapper) extends Thread {
  override def run(): Unit = {
    if (data.isDirectory) {
      data.listFiles().foreach {
        sub =>
          Thread.sleep(50)
          new Runner("sub-" + name, sub).start()
      }
    } else {

      val stream = new TXTFileLogReader("/SZ_test" + data.name, None, None, data, new ReadPosition("/SZ_test" + data.name, 0, 0))
      print(name + ":" + data.name)
      stream.foreach {
        event =>
          TimeUnit.SECONDS.sleep(1)
          println(name + ":" + event)
      }
    }


  }
}
