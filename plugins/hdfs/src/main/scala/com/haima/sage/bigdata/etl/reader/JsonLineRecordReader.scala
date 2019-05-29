package com.haima.sage.bigdata.etl.reader

import java.io.IOException
import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Seekable}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
import org.apache.hadoop.mapred.{FileSplit, RecordReader}
import org.apache.hadoop.mapreduce.lib.input.{CompressedSplitLineReader, SplitLineReader}

/**
  * Created by evan on 17-9-12.
  *
  * recognize json string in one line
  *
  */
class JsonLineRecordReader(job: Configuration, split: FileSplit, recordDelimiter: Array[Byte]) extends RecordReader[LongWritable, util.Map[String, Object]] with Serializable {
  private val emptyFilters = new Array[SerializeFilter](0)
  private val LOG = LogFactory.getLog(classOf[JsonLineRecordReader].getName)
  private var compressionCodecs: CompressionCodecFactory = null
  private var start = 0L
  private var pos = 0L
  private var end = 0L
  private var in: SplitLineReader = null
  private var fileIn: FSDataInputStream = null
  private var filePosition: Seekable = null
  private var maxLineLength = 0
  private var codec: CompressionCodec = null
  private var decompressor: Decompressor = null

  maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647)
  start = split.getStart
  end = start + split.getLength
  val file = split.getPath
  compressionCodecs = new CompressionCodecFactory(job)
  codec = compressionCodecs.getCodec(file)
  val fs = file.getFileSystem(job)
  fileIn = fs.open(file)
  if (isCompressedInput) {
    decompressor = CodecPool.getDecompressor(codec)
    if (codec.isInstanceOf[SplittableCompressionCodec]) {
      val cIn = codec.asInstanceOf[SplittableCompressionCodec].createInputStream(fileIn, decompressor, start, end, READ_MODE.BYBLOCK)
      in = new CompressedSplitLineReader(cIn, job, recordDelimiter)
      start = cIn.getAdjustedStart
      end = cIn.getAdjustedEnd
      filePosition = cIn
    }
    else {
      in = new SplitLineReader(codec.createInputStream(fileIn, decompressor), job, recordDelimiter)
      filePosition = fileIn
    }
  }
  else {
    fileIn.seek(start)
    in = new SplitLineReader(fileIn, job, recordDelimiter)
    filePosition = fileIn
  }
  if (start != 0L) start += in.readLine(new Text, 0, maxBytesToConsume(start)).toLong
  pos = start

  override def createKey = new LongWritable

  override def createValue = new util.HashMap[String, Object]()

  private def isCompressedInput = codec != null

  private def maxBytesToConsume(pos: Long) = if (isCompressedInput) 2147483647
  else Math.min(2147483647L, end - pos).toInt

  @throws[IOException]
  private def getFilePosition = {
    var retVal = 0L
    if (isCompressedInput && null != filePosition) retVal = filePosition.getPos
    else retVal = pos
    retVal
  }

  @throws[IOException]
  override def next(k: LongWritable, v: util.Map[String, Object]): Boolean = {
    val value: Text = new Text()
    value.set(JSON.toJSONString(v, emptyFilters))
    val hasNext = next(k, value)
    if (hasNext) {
      if (value != null && value.toString != null && !value.toString.trim.equals("")){
        try {

          /*
          lexer.parser(value)
          */
          val data: util.Map[String, Object] = JSON.parseObject(value.toString).toJavaObject(classOf[util.Map[String, Object]])
          v.putAll(data)
        } catch {
          case ex: Exception =>
            LOG.warn(s"read line[${value.toString}] error:", ex)
            v.clear()
        }
      } else {
        v.clear()
      }
    }
    hasNext
  }

  @throws[IOException]
  def next(key: LongWritable, value: Text): Boolean = {
    while (getFilePosition <= end || in.needAdditionalRecordAfterSplit) {
      key.set(pos)
      /*codec*/
      val newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength))
      if (newSize == 0) return false
      pos += newSize.toLong
      if (newSize < maxLineLength) return true
      //      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize.toLong))
    }
    false
  }

  @throws[IOException]
  override def getProgress: Float = if (start == end) 0.0F
  else Math.min(1.0F, (getFilePosition - start).toFloat / (end - start).toFloat)

  @throws[IOException]
  override def getPos: Long = {
    return pos
  }

  @throws[IOException]
  override def close(): Unit = {
    try {
      if (in != null) {
        in.close()
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor)
      }
    }
  }
}
