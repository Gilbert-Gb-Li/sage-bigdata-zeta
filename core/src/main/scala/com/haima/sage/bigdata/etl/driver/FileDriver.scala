package com.haima.sage.bigdata.etl.driver

import java.io.File

import scala.util.Try

/**
  * Created by evan on 17-8-11.
  */
case class FileDriver(mate: FileMate) extends Driver[File] {
  override def driver(): Try[File] = Try(new File(mate.path))
}
