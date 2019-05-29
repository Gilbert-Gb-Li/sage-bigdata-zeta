package com.haima.sage.bigdata.etl.common.model

import java.io._

import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.utils.{FileUtils, Logger}


/**
  * Created by zhhuiyan on 15/5/22.
  */
trait FileWrapper[T] extends Logger {

  private val AGENT_FILE_READER_THREAD_BUFFER: Integer = CONF.getInt(READER_FILE_BUFFER) match {
    case 0 =>
      READER_FILE_BUFFER_DEFAULT
    case a: Int =>
      a
  }

  def inputStream: InputStream

  protected val utils: FileUtils

  def _encoding: Option[String]

  def exists(): Boolean

  def isDirectory: Boolean

  def listFiles(): Iterator[T]

  def name: String

  def absolutePath: String

  def uri: String


  def metadata: Map[String, Any]

  def skip(num: Long): Long = {
    utils.skip(bufferedInputStream, num)
  }

  lazy val encoding: String = {
    _encoding match {
      case Some(enc: String) if !"".equals(enc) =>
        enc
      case _ =>
        utils.charset(bufferedInputStream)

    }
  }
  private lazy val bufferedInputStream: BufferedInputStream = new BufferedInputStream(inputStream, AGENT_FILE_READER_THREAD_BUFFER)
  lazy final val stream: ReaderLineReader = {
    try {
      ReaderLineReader(new BufferedReader(new InputStreamReader(bufferedInputStream, encoding), AGENT_FILE_READER_THREAD_BUFFER))

    } catch {
      case _: Exception =>
        null
    }
  }

  // lazy final val lineSeparator: String = utils.lineSeparator(stream)

  def skipLine(num: Int): (Long, Long) = {

    (0 until num).map {
      _ =>
        val line = stream.readLine()
        if (line != null) {
          (1l, line.length().toLong)
        } else {
          (0l, 0l)
        }
    }.reduce[(Long, Long)] {
      case (f, second) =>
        (f._1 + second._1, f._2 + second._2)
    }
  }

  def lastModifiedTime: Long

  def close(): Unit = {

  }
}

case class ReaderLineReader(var bufferedReader: BufferedReader) {

  @throws[IOException]
  def skip(n: Long): Long = bufferedReader.skip(n)

  @throws[IOException]
  def readLine(): String = {
    val sb = new StringBuilder
    do {
      var c = bufferedReader.read
      if (c == -1)
        if (sb.isEmpty)
          return null
        else return sb.toString
      sb.append(c.toChar)
      if (c == '\n' || c == '\u2028' || c == '\u2029' || c == '\u0085')
        return sb.toString
      if (c == '\r') {
        bufferedReader.mark(1)
        c = bufferedReader.read
        if (c == -1)
          return sb.toString
        else if (c == '\n')
          sb.append('\n')
        else
          bufferedReader.reset()
        return sb.toString
      }
    } while (true)
    sb.toString
  }

  @throws[IOException]
  def close(): Unit = {
    bufferedReader.close()
  }
}