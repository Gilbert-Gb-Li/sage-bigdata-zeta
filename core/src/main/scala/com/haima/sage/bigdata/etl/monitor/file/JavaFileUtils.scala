package com.haima.sage.bigdata.etl.monitor.file

import java.io._

import com.haima.sage.bigdata.etl.utils.FileUtils

/**
  * Created by zhhuiyan on 16/5/26.
  */
object JavaFileUtils extends FileUtils {
  @throws[IOException]
  override def reset(reader: Reader): Unit = {
    if (reader.markSupported) {
      reader.reset()
    }

  }

  @throws[IOException]
  override def reset(reader: InputStream): Unit = {
    if (reader.markSupported) {
      reader.reset()
    }

  }

  @throws[IOException]
  override def skip(reader: InputStream, position: Long): Long = {
    reader.skip(position)
  }
}
