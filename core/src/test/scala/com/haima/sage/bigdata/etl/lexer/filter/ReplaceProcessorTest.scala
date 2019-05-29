package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Replace
import org.junit.Test

class ReplaceProcessorTest {

  @Test
  def testRegNum(): Unit = {
    val processor = new ReplaceProcessor(Replace("fans","""\s*(粉丝)?\s*(\d+)\s*万\s*(粉丝)?\s*""", "$20000"))
    val data = List("粉丝 10 万"," 粉丝 10 万"," 粉丝 10 万 ",
      "粉丝 10万",
      "粉丝10万", "10 万粉丝", "10 万 粉丝", "10万粉丝", " 10万粉丝", " 10万粉丝 ","116205粉丝")
    val rt=data.map(v=>RichMap(Map("fans"->v))).map(processor.process)
    rt.foreach(println)
    assert(  rt.forall(t=>t("fans")=="100000"))
  }
  @Test
  def testRegNum2(): Unit = {
    val processor = new ReplaceProcessor(Replace("fans","""\s*(粉丝)?\s*(\d+)\s*(粉丝)?\s*""", "$2"))
    val data = List("粉丝 10 "," 粉丝 10 "," 粉丝 10 ",
      "粉丝 10",
      "粉丝10", "10 粉丝", "10 粉丝", "10粉丝", " 10粉丝", " 10粉丝 ","116205粉丝")
    val rt=data.map(v=>RichMap(Map("fans"->v))).map(processor.process)
    rt.foreach(println)
    assert(  rt.forall(t=>t("fans").toString.matches("\\d+")))
  }

}
