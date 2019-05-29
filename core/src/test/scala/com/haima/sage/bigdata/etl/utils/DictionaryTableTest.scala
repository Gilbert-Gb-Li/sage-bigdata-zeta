package com.haima.sage.bigdata.etl.utils

import org.junit.Test

class DictionaryTableTest {
  @Test
  def testIp(): Unit = {

    assert(DictionaryTable.find("172.16.219.130").isEmpty)
    assert(DictionaryTable.find("192.168.219.130").isEmpty)
    assert(DictionaryTable.find("10.168.219.130").isEmpty)
    assert(DictionaryTable.find("101.168.219.130").nonEmpty, s"")
  }
}
