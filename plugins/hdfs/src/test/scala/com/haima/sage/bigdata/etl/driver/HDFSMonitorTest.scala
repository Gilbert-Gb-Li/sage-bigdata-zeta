package com.haima.sage.bigdata.etl.driver

import com.haima.sage.bigdata.etl.codec.LineCodec
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.driver.usable.HDFSUsabilityChecker
import org.junit.Test

/**
  * Created by bbtru on 2017/10/10.
  */
class HDFSMonitorTest {

  @Test
  def test(): Unit = {
    val fileSource = new FileSource("/hbase/text.txt", Some("other"),
      Some(new TxtFileType()), None, Some(new LineCodec()), Some(ProcessFrom.END),0)
    val mate = new HDFSSource("HaimaDev", "nn1,nn2","bigdata-dev01:8020,bigdata-dev02:8020", Some(AuthType.NONE), fileSource)
    val hdfs = new HDFSUsabilityChecker(mate)
    val usability = hdfs.check


  }

}
