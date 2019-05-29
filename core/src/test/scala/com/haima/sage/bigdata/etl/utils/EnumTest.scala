package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.model.Opt
import org.junit.Test

/**
  * Created by zhhuiyan on 2017/5/15.
  */
class EnumTest {
  @Test
def control(): Unit ={

  assert(Opt.withName("stop".trim.toUpperCase)==Opt.STOP,"Enum.withName must be ok")
  }
}
