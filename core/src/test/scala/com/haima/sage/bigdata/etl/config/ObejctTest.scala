package com.haima.sage.bigdata.etl.config

import com.haima.sage.bigdata.etl.authority.Authorization._
import com.haima.sage.bigdata.etl.authority.Resource._
import com.haima.sage.bigdata.etl.authority._
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

/**
  * Created by zhhuiyan on 2016/10/11.
  */
class ObjectTest extends Mapper {

  @Test
  def testMapping(): Unit = {
    println(mapper.writeValueAsString(Start("12222")))
    println(mapper.writeValueAsString(UPDATE))
    println(mapper.readValue[Authorization](mapper.writeValueAsString(UPDATE)))
    println(mapper.readValue[Authorization](mapper.writeValueAsString(Start("12222"))))
    println(mapper.writeValueAsString(ASSET))
    //println(mapper.writeValueAsString(Admin))
  }
}
