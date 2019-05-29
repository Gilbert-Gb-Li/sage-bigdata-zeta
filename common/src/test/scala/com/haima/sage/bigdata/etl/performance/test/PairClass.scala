package com.haima.sage.bigdata.etl.performance.test

class PairClass {
  var name: StringBuilder = new StringBuilder
}

object PairClass{
  val a = new PairClass
  def printPair(): Unit ={
    a.name ++= ",abc"
    println(a,a.name)
  }
}

object MainStart extends App {

  for(i <- 1 to 3)
  PairClass.printPair()
}