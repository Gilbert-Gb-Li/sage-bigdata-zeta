package com.haima.sage.bigdata.etl.store.position

import java.io._
import java.util
import java.util.StringTokenizer
import java.util.concurrent.ConcurrentHashMap

import com.haima.sage.bigdata.etl.common.model.ReadPosition
import org.slf4j.{Logger, LoggerFactory}


class FileReadPositionStore extends ReadPositionStore {
  @volatile
  private var position: ReadPosition = _

  private[store] val logger: Logger = LoggerFactory.getLogger(classOf[FileReadPositionStore])
  private var store: util.Map[String, ReadPosition] = _
  private val LINE_SEPARATE: String = "|"

  private val fileName: String = "./db/pos.2"

  @SuppressWarnings(Array("resource"))
  @throws(classOf[IOException])
  protected def readMap(pathname: String): util.Map[String, ReadPosition] = {
    val map: util.Map[String, ReadPosition] = new ConcurrentHashMap[String, ReadPosition]
    val file: File = new File(pathname)
    if (!file.exists) {
      if (!file.createNewFile) {
        throw new IOException("cannot create file to store")
      }
    }
    val in: BufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(pathname), "UTF-8"))
    try {
      var line: String = in.readLine
      while (line != null) {
        var path: String = null
        var pos: Long = 0L
        var lineNumber: Long = 0L
        val st: StringTokenizer = new StringTokenizer(line, LINE_SEPARATE)
        path = st.nextToken
        lineNumber = st.nextToken.trim.toLong
        pos = st.nextToken.trim.toLong
        if (path != null && pos > 0 && lineNumber > 0) {
          map.put(path, ReadPosition(path, lineNumber, pos))
        }
        line = in.readLine
      }
      map
    } finally {
      try {
        in.close()
      }
      catch {
        case ignored: Exception =>
      }
    }

  }

  @throws(classOf[IOException])
  protected def writeMap(pathname: String, map: util.Map[String, ReadPosition]) {
    val out: PrintWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(pathname), "UTF-8"))
    import scala.collection.JavaConversions._

    map.entrySet.foreach(entry =>
      out.println(entry.getKey + LINE_SEPARATE + entry.getValue.records + LINE_SEPARATE + entry.getValue.position))
    out.close()
  }

  private var offset: Int = 0

  def init: Boolean = {
    try {
      store = readMap(fileName)
      true
    }
    catch {
      case ex: IOException =>
        ex.printStackTrace()
        false
    }
  }

  def close() {
    try {
      flush()
      store.clear()
    }
    catch {
      case e: IOException =>
        logger.error("read position pos store error,when flush to file:{}", e)
    }
  }

  def set(readPosition: ReadPosition): Boolean = {
    store.put(readPosition.path, readPosition)
    try {
      flush()
    }
    catch {
      case e: IOException =>
        logger.error("read position pos store error,when flush to file:{}", e)
        return false
    }
    true
  }

  def setCacheSize(size: Int) {
    this.offset = size
  }

  def remove(path: String): Boolean = {
    try {
      store.remove(path)
      flush()
      true
    }
    catch {
      case e: IOException =>
        logger.error(s"read_position store error,when delete $path:{}", e)
        false
    }
  }

  override def get(path: String): Option[ReadPosition] = {
    store.get(path) match {
      case position: ReadPosition =>
        Some(position)
      case _ =>
        None

    }
  }

  @throws(classOf[IOException])
  def flush() {
    writeMap(fileName, store)
  }

  override def list(path: String): List[ReadPosition] = {
    import scala.collection.JavaConversions._
    store.filter(_._1.startsWith(path)).values.toList
  }

  def fuzzyRemove(path: String): Boolean = { false }

}
