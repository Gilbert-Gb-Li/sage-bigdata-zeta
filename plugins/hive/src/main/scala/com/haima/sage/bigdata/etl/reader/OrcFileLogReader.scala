package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.exception.LogReaderInitException
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.monitor.file.HDFSFileWrapper
import com.haima.sage.bigdata.etl.stream.OrcStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.orc.OrcFile

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/8/1 10:01.
  */
class OrcFileLogReader(uri: String, codec: Option[Codec], contentType: Option[ORCFileType] = None, wrapper: FileWrapper[_], position: ReadPosition) extends
  FileLogReader[RichMap](uri, codec, contentType, wrapper, position) {

  lazy val configuration = new Configuration
  configuration.setBoolean("dfs.support.append", true)
  configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
  configuration.set("dfs.block.access.token.enable", "true")
  configuration.set("dfs.http.policy", "HTTP_ONLY")
  configuration.set("dfs.replication", "1")
  configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  configuration.set("fs.hdfs.impl.disable.cache", "true")
  lazy val reader = OrcFile.createReader(wrapper.asInstanceOf[HDFSFileWrapper].path,
    OrcFile.readerOptions(configuration).filesystem(wrapper.asInstanceOf[HDFSFileWrapper].fs))


  lazy val rows = reader.rows

  import scala.collection.JavaConverters._

  lazy val fields = reader.getSchema.getFieldNames.asScala.toList
  lazy val batch: VectorizedRowBatch = reader.getSchema.createRowBatch

  //  override val stream: Stream[RichMap] = ExcelStream(path,position.position)

  override lazy val stream: Stream[RichMap] = OrcStream(rows, batch, fields)

  override lazy val iterator: Iterator[RichMap] = new Iterator[RichMap] {
    override def next(): RichMap = {
      position.positionIncrement(1)
      position.recordIncrement()
      stream.next()
    }

    override def hasNext: Boolean = {
      val has = stream.hasNext
      if (!has) {
        position.finished = true
      }
      has

    }
  }

  def skip(skip: Long): Long = {

    if (skip == 0) {
      0
    } else {

      if (rows.getRowNumber < skip) {
        0
      } else {
        rows.seekToRow(skip)
        skip
      }

    }

  }

  override def skipLine(length: Long): Unit = {
    if (skip(length) < length - 1) {
      close()
      throw new LogReaderInitException(s"$path has process finished,don't need to  read again. records:${position.records}")
    }
  }

  override def close(): Unit = {
    super.close()
  }
}
