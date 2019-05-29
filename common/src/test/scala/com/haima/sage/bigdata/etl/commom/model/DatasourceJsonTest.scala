package com.haima.sage.bigdata.etl.commom.model

import com.haima.sage.bigdata.etl.codec.MultiCodec
import com.haima.sage.bigdata.etl.common.model.FileSource
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

import scala.util.Try


/**
  * Created by zhhuiyan on 2017/4/20.
  */
class DatasourceJsonTest extends Mapper{

  @Test
  def file(): Unit ={
   val source=Some(FileSource("data/av_log/SystemOut.log", Option("SystemOut"),
     codec = Some(MultiCodec("[\\[]", Some(true), None))))

    assert(Try(mapper.writeValueAsString(source)).isSuccess)
    val json=  mapper.writeValueAsString(source)
    assert(Try(mapper.readValue[Option[FileSource]](json)).isSuccess)
  }

}
