package com.haima.sage.bigdata.etl

import com.haima.sage.bigdata.etl.common.model.writer.{NameType, RefNone}
import com.haima.sage.bigdata.etl.common.model.{ES5Writer}
import com.haima.sage.bigdata.etl.driver.ElasticSearchMate
import com.haima.sage.bigdata.etl.plugin.es_5.driver.usable.ESUsabilityChecker
import org.junit.Test

/**
  * Created by bbtru on 2017/10/23.
  */
class ESUsabilityCheckerTest {

  @Test
  def testCheck(): Unit = {
    val metadata: Option[List[(String, String, String)]] = Some(List(("raw", "string", "")))
    val hostPorts: Array[(String, Int)]  = Array(("10.10.100.71",9200))
    val persisRef: Option[NameType] = Some(new RefNone())
    val mate: ElasticSearchMate = new ES5Writer(null, "zdp_es_cluster", hostPorts, "test", "typea",
      None, None, None, None, 5, 0, false, true, true, persisRef, metadata, "es5", 100)
    val esuc = new ESUsabilityChecker(mate)
    assert(esuc.check.usable == true, "can not connect es5.")
  }

}
