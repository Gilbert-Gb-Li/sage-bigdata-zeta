package com.haima.sage.bigdata.etl.utils

import java.io._
import java.util

import com.ibm.icu.text.{CharsetDetector, CharsetMatch}

/**
  * Created by zhhuiyan on 16/5/26.
  */
trait FileUtils {
  private val detector: CharsetDetector = new CharsetDetector

  @throws[IOException]
  def reset(reader: Reader): Unit

  @throws[IOException]
  def reset(reader: InputStream): Unit

  @throws[IOException]
  def skip(reader: InputStream, position: Long): Long

  @throws[IOException]
  def charset(reader: InputStream): String = {
    val size: Int = 8192
    if (reader.markSupported) {
      reader.mark(1024)
    }
    val input: Array[Byte] = new Array[Byte](size)
    var length: Int = 0
    var remainingLength: Int = size
    var bytesRead: Int = 1
    while (remainingLength > 0 && bytesRead > 0) {
      {
        bytesRead = reader.read(input, length, remainingLength)
        if (bytesRead > 0) {
          length += bytesRead
          remainingLength -= bytesRead
        }
      }
    }
    reset(reader)
    var encoding: String = null
    val matcher: CharsetMatch = detector.setText(input).detect
    if (matcher == null) {
      val os: String = System.getProperty("os.name")
      if (os.toLowerCase.contains("windows")) encoding = "GB18030"
      else {
        encoding = "UTF-8"
      }
    }
    else {
      encoding = matcher.getName
    }
    encoding
  }

  @throws[IOException]
  def lineSeparator(reader: BufferedReader): String = {
    if (reader.markSupported) {
      reader.mark(1024 * 1024 * 8)
    }
    var line: String = reader.readLine
    if (line == null) {
      reset(reader)
      return System.lineSeparator
    }
    reset(reader)
    var chars: Array[Char] = new Array[Char](line.length)
    reader.read(chars)
    chars = new Array[Char](3)
    reader.read(chars)
    reset(reader)
    reader.readLine
    line = reader.readLine
    reset(reader)
    if (line == null) {
      return System.lineSeparator
    }
    var j: Int = 0
    if (line != "") {
      val start: Char = line.charAt(0)

      (0 until 3).foreach { i =>
        if (start == chars(i)) {
          j = i
        }
      }
    }

    new String(util.Arrays.copyOf(chars,
      if (j == 0) 1 else j))
  }


}
