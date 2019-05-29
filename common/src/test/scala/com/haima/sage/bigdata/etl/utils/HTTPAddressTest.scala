package com.haima.sage.bigdata.etl.utils


import org.junit.Test

class HTTPAddressTest {

  @Test
  def withProtocolURL(): Unit ={

   assert(  HTTPAddress("http://127.0.0.1:19090/ui/index.html#!/").orNull ==
     HTTPAddress(Some("http"),"127.0.0.1",19090,Some("ui/index.html#!/")))
  }
  @Test
  def withProtocol(): Unit ={

    assert(  HTTPAddress("http://127.0.0.1:19090").orNull ==
      HTTPAddress(Some("http"),"127.0.0.1",19090))
    assert(  HTTPAddress("http://127.0.0.1:19090/").orNull ==
      HTTPAddress(Some("http"),"127.0.0.1",19090))
  }
  @Test
  def withURL(): Unit ={

    assert(  HTTPAddress("127.0.0.1:19090/ui/index.html#!/").orNull ==
      HTTPAddress(None,"127.0.0.1",19090,Some("ui/index.html#!/")))
  }
}
