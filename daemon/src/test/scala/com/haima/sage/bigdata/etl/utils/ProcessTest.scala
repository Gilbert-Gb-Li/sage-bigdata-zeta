package com.haima.sage.bigdata.etl.utils

import org.junit.Test

/**
  * Created by zhhuiyan on 2017/5/9.
  */
class ProcessTest {
  import sys.process._
  @Test
def `ls-l`(): Unit ={

    println(s"load from[${("pwd " !!).trim}]")

    println(sys.env("JAVA_HOME"))
    Seq("bash", "-c", "echo `$JAVA_HOME`").lineStream_!.foreach(println)
   println("ls -l " !)
  }
  @Test
  def exec(): Unit ={
    "sage-bigdata-etl/target/releases/sage-bigdata-etl-worker-1.0.0-SNAPSHOT/bin/start.sh".lineStream_!.foreach(println)
  }
  @Test
  def exec2(): Unit ={
    "cmd /c E:\\sage-bigdata-etl-2.5.3.1\\bin\\start-worker.bat".lineStream_!.foreach(println)
  }


@Test
  def path(): Unit ={
  val paths=this.getClass.getResource("/").getPath.split("/")
  println(paths.mkString("/"))
  println(paths.slice(0,paths.length-1).mkString("/"))
}

  @Test
  def getOSInfo(): Unit = {
    val os = System.getProperty("os.name").toLowerCase()
    println(os)
  }


}


