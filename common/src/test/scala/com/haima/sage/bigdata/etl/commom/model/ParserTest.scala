package com.haima.sage.bigdata.etl.commom.model

import com.haima.sage.bigdata.etl.common.model.Parser
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

/**
  * Created by zhhuiyan on 2017/4/21.
  */
class ParserTest extends Mapper{
  @Test
  def mapping(): Unit ={

   println( mapper.writeValueAsString(Parser("xml")))
  }

}
