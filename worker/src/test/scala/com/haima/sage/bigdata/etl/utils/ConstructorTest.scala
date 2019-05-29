package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.model.{ES2Source, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.ElasticSearchMate
import org.junit.Test

import scala.util.Try

/**
  * Created by zhhuiyan on 2017/4/20.
  */
class ConstructorTest {

  @Test
  def test(): Unit = {
    val mate = ES2Source("cluster", Array(), "index", "type", "field", "00", 10)
    val constructors = Class.forName("com.haima.sage.bigdata.etl.driver.usable.ESUsabilityChecker").getConstructors
    assert(Try(constructors.head.newInstance(mate)).isSuccess)
    assert(Try(Class.forName("com.haima.sage.bigdata.etl.driver.usable.ESUsabilityChecker").getConstructor(mate.getClass.getSuperclass).newInstance(mate).asInstanceOf[UsabilityChecker]).isFailure)
    assert(Try(Class.forName("com.haima.sage.bigdata.etl.driver.usable.ESUsabilityChecker").getConstructor(classOf[ElasticSearchMate]).newInstance(mate).asInstanceOf[UsabilityChecker]).isSuccess)


  }
}
