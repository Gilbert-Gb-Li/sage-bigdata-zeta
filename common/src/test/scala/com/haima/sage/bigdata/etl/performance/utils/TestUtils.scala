package com.haima.sage.bigdata.etl.performance.utils

import java.io.{File, FileInputStream}

import scala.io.Source

object TestUtils {
  def readAll(str: String, encoding: String = "UTF-8"): String = {
    val in = getClass.getClassLoader.getResourceAsStream(str)
    val source = Source.fromInputStream(in, encoding)
    source.mkString
  }

  def readIgnoreFirst(str: File, encoding: String = "UTF-8", ignoreFirst: Boolean = true): String = {
    val in = new FileInputStream(str)
    val source = Source.fromInputStream(in, encoding)
    if (ignoreFirst) {
      val iterator = source.getLines()
      iterator.next()
      iterator.mkString("\n")
    } else {
      source.mkString
    }
  }
}
