package com.haima.sage.bigdata.etl.common.model

import java.io
import java.io._

class RichFile(file: File) {
  def bufferedWriter(append: Boolean = false): BufferedWriter = {
    new BufferedWriter(new io.FileWriter(file, append))
  }

  def outputStream(append: Boolean = false): FileOutputStream = {
    new FileOutputStream(file, append)
  }

  def inputStream(): FileInputStream = {
    new FileInputStream(file)
  }

  def bufferedReader(): BufferedReader = {
    new BufferedReader(new FileReader(file))
  }
}

object RichFile {

  import com.haima.sage.bigdata.etl.common.Implicits._

  def apply(file: File): RichFile = file

  def apply(path: String): RichFile = new File(path)
}