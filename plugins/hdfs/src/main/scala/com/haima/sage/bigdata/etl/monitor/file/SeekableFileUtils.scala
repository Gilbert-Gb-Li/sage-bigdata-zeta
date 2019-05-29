package com.haima.sage.bigdata.etl.monitor.file

import java.io._

import com.haima.sage.bigdata.etl.utils.FileUtils
import org.apache.hadoop.fs.Seekable

/**
  * Created by zhhuiyan on 16/5/26.
  */
object SeekableFileUtils extends FileUtils {
  @throws[IOException]
  override def reset(reader: Reader): Unit = {

    reader match {
      case seek: Seekable =>
        seek.seek(0)
      case _ =>
        if (reader.markSupported) {
          reader.reset()
        }else{
        }

    }

  }

  @throws[IOException]
  override def reset(reader: InputStream): Unit = {
    reader match {
      case seek: Seekable =>
        seek.seek(0)
      case _ =>
        if (reader.markSupported) {
          reader.reset()
        }else{
        }
    }

  }

  @throws[IOException]
  override def skip(reader: InputStream, position: Long): Long = {
    reader match {
      case seek: Seekable =>
        seek.seek(position)
        seek.getPos
      case _ =>
        reader.skip(position)

    }

  }
}
