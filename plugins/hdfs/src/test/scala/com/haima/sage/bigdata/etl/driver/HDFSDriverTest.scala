package com.haima.sage.bigdata.etl.driver

import com.haima.sage.bigdata.etl.codec.LineCodec
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.driver.usable.HDFSUsabilityChecker
import org.junit.Test

/**
  * Created by bbtru on 2017/10/10.
  */
class HDFSDriverTest {

  /**
    * 测试HDFSDriver驱动类
    * 说明: 权限校验为NONE
    */
  @Test
  def testHdfsDriver(): Unit = {
    val fileSource = new FileSource("/data/takeout/snapshot/table_takeout_active_shop/dt=2018-08-2", Some("other"),
      Some(new TxtFileType()), None, Some(new LineCodec()), Some(ProcessFrom.END),0)
    val mate = new HDFSSource("HaimaDev", "nn1,nn2","bigdata-dev01:8020,bigdata-dev02:8020", Some(AuthType.NONE), fileSource)
    val hdfs = new HDFSUsabilityChecker(mate)
    val usability = hdfs.check
    assert(usability.usable, "驱动创建不成功.")
  }

  /**
    * 测试HDFSDriver驱动类
    * 说明: 权限校验为KERBEROS
    */
  @Test
  def testHdfsDriverKer(): Unit = {
    val fileSource = new FileSource("/hbase/text.txt", Some("other"),
      Some(new TxtFileType()), None, Some(new LineCodec()), Some(ProcessFrom.END),0)
    val mate = new HDFSSource("HaimaDev", "nn1,nn2","bigdata-dev01:8020,bigdata-dev02:8020", Some(AuthType.NONE), fileSource)
    val hdfs = new HDFSUsabilityChecker(mate)
    val usability = hdfs.check
    assert(usability.usable, "驱动创建不成功.")
  }

}
