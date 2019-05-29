package com.haima.sage.bigdata.etl.commom.model

import com.haima.sage.bigdata.etl.common.model.Collectors
import org.junit.Test

class CollectorTest {
  @Test
  def from (): Unit ={
    assert(Collectors.from("akka.tcp://worker@127.0.0.1:19093/user/relay-server").isDefined)
    assert(Collectors.from("akka.tcp://worker@127.0.0.1:19093").isDefined)
  }

}
