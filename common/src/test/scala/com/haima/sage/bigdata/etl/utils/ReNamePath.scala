package com.haima.sage.bigdata.etl.utils

import org.junit.Test
import java.io._

import com.haima.sage.bigdata.etl.common.model.BuildResult

import scala.io.Source
import scala.util.{Failure, Success, Try}
import sys.process._
class ReNamePath {

  @Test
  def refactor(): Unit ={
    val file=new File("/Users/zhhuiyan/workspace/sage-bigdata-etl")
    file.listFiles().toList.filter(_.isDirectory).filter(
      _.listFiles().toList.map(_.getName).contains[String]("src")
    ).foreach(t=>{

      val mvMain=s"rm -r  ${t.getAbsolutePath}/src/main/scala/com/raysdata"
      val mvTest=s"rm -r ${t.getAbsolutePath}/src/test/scala/com/raysdata"

      exec(mvMain)
      exec(mvTest)

    })

    def exec(main:String): Unit ={
      Try(main.lineStream_!.fold("")((left, right) => left + right)) match {
        case Success(msg) =>
          println(msg)
        case Failure(e) =>
          e.printStackTrace()

      }
    }

  }

}
