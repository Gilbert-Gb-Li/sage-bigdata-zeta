package com.haima.sage.bigdata.etl.quartz

import scala.util.{Failure, Success, Try}


object ScalaTest extends App {

//  val str = "a,b,c,d"
//
//  val arr = str.slice(0, str.length).replace(",", "")
//
//  val (name, age) = if(1 > 0){
//    (true,23)
//  }else{
//    throw new Exception("123")
//  }
//
//
//  val res = Array("0","2")
//  val b = "a,b,c,d"
//  val a = b.slice(0, 1)
//
//  val s = ScalaTest(name = "zhangsan")
//
//  val a1 = "xiaoming"
//  val a2 = "xiaozhang"
//  val a3 = "xiaoming"
//  a1 match {
//    case `a2` => println("a2")
//    case `a3` => print("a1")
//  }

//  val event: Map[String, String] = Map("location" -> "中国 • 20天前")
//  val local = event.getOrElse("location", "unknow")
//  if ("unknow" != local) {
//    event + ("location" -> local.split(" ")(0) )
//  } else {
//    event + ("location" -> "unknow")
//  }

  val str = "hdfs://HaimaDev/data/douyin"

  val a = str.split("/")
  val b = s"/${a.slice(3,a.length).mkString("/")}"
  println(b)



}

case class ScalaTest(id: Int = 0, name: String, age: Int = 20)





