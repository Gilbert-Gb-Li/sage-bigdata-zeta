package com.haima.sage.bigdata.etl.input

import java.io.IOException
import java.nio.charset.Charset
import java.util

import com.haima.sage.bigdata.etl.reader.JsonLineRecordReader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapred._

/**
  * Created by evan on 17-9-12.
  *
  * recognize json string in one line.
  *
  */
class JsonInputFormat() extends FileInputFormat[LongWritable, util.Map[String, Object]] with JobConfigurable with Serializable {
  private var compressionCodecs: CompressionCodecFactory = null

  def configure(conf: JobConf) {
    this.compressionCodecs = new CompressionCodecFactory(conf)
  }

  override protected def isSplitable(fs: FileSystem, file: Path): Boolean = {
    val codec: CompressionCodec = this.compressionCodecs.getCodec(file)
    if (null == codec) true
    else codec.isInstanceOf[SplittableCompressionCodec]
  }

  @throws[IOException]
  def getRecordReader(genericSplit: InputSplit, job: JobConf, reporter: Reporter): RecordReader[LongWritable, util.Map[String, Object]] = {
    reporter.setStatus(genericSplit.toString)
    val delimiter: String = job.get("textinputformat.record.delimiter")
    var recordDelimiterBytes: Array[Byte] = null

    if (null != delimiter) recordDelimiterBytes = delimiter.getBytes(Charset.forName("UTF-8"))
    new JsonLineRecordReader(job, genericSplit.asInstanceOf[FileSplit], recordDelimiterBytes)
  }
}
