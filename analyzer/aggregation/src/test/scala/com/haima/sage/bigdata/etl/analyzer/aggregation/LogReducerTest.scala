package com.haima.sage.bigdata.analyzer.aggregation

import java.io.IOException

import com.haima.sage.bigdata.analyzer.aggregation.model.{LogReducer, Pattern, Terms}
import com.haima.sage.bigdata.etl.utils.Logger
import org.junit.Test


/**
  * Created by wxn on 2017/10/23.
  */
class LogReducerTest extends Logger {

  @Test
  def segmentTextTest(): Unit = {
    var data = ""
    var result = List[String]()
    data = "101.2.3.4"
    result = Terms(data).terms
    println(result.toString)
    assert(result.size == 7)


    data = "IP 101.2.3.4"
    result = Terms(data).terms
    println(result.toString)
    assert(result.size == 9)

    data = "IP 101.2.3.4 中文"
    result = Terms(data).terms
    println(result.toString)
    assert(result.size == 11)

    data = "2015-04-03 15:55:39,1428047739899719," + "LBMP-APP41-LBS,遍历无效,,N/A,仅检测: 重置,100,传入,连接流,00:50:56:8E:1E:6A," + "TCP,ACK PSH DF=1,10.64.68.26,12:01:9B:00:12:00,60950,172.17.5.67," + "00:50:56:8E:1E:6A,7001,358,\"uri-normalize\",1,,26,26,8,6," + "474554202F25356325326525326525356325326525326525356325326525" + "3265253563626F6F742E696E6920485454502F312E310D0A436F6E6E6563746" + "96F6E3A20436C6F73650D0A486F73743A203137322E31372E352E36370D0A507" + "261676D613A206E6F2D63616368650D0A557365722D4167656E743A204D6F7A696" + "C6C612F342E3735205B656E5D20285831312C20553B2052736173290D0A416363657" + "0743A20696D6167652F6769662C20696D6167652F782D786269746D61702C20696D6" + "167652F6A7065672C20696D6167652F706A7065672C20696D6167652F706E672C202A" + "2F2A0D0A4163636570742D4C616E67756167653A20656E0D0A4163636570742D4368617" + "27365743A2069736F2D383835392D312C2A2C7574662D380D0A0D0A"
    result = Terms(data).terms
    println(result.toString)
    assert(result.size == 124)
  }

  @Test
  def segmentTest(): Unit = {
    val list: List[String] = Terms("警报: 防恶意软件引擎脱机\n严重性: 严重").terms

    println(list.mkString("['", "','", "']"))
  }


  @Test
  def regressionTestIssue22(): Unit = {
    try {
      val list: List[String] = Terms("警报: 防恶意软件引擎脱机\n严重性: 严重").terms
      println(list.toString())
      assert(list.indexOf("\n") > 0)

      var rows: List[String] = List[String]()
      //      rows = rows.+("警报: 防恶意软件引擎脱机\n严重性: 普通")
      //        .+("告警: 防恶意软件引擎脱机\n严重性: 普通")
      //        .+("警报: 入侵防御引擎脱机\n严重性: 严重")
      //        .+("告警: 防火墙引擎脱机\n严重性: 严重")

      val logReducer: LogReducer = new LogReducer(0.7)
      val result: Map[List[Pattern], String] = logReducer.runLogReducer(rows)
      println("==============================================================================")
      assert(result.size == 1)

      println("result size: " + result.size)
      println("-------------")
      println("result info: " + result.toString())


      println("-------------")

      val patternList: List[Pattern] = result.keys.iterator.next()
      println("==============================================================================")
      //assert(patternList.size == 1)
      println("patternList size: " + patternList.size)
      assert(patternList.size == 2)
      println("patternList info: " + patternList.toString())
      println("patternList.head.getPattern.size: " + patternList.head.pattern.size)
      assert(patternList.head.pattern.size == 2)
      println("patternList.head.total: " + patternList.head.total)
      println("==============================================================================")

      println("patternList.head.getPattern: " + patternList.head.pattern)
      println("patternList.head.getPatternParam: " + patternList.head.params)
      println("patternList.head.getPatternText: " + patternList.head.texts)
      println("patternList.head.toString: " + patternList.head.toString)
      println("patternList.head.toKey: " + patternList.head.toKey)
    } catch {
      case e: IOException =>
        logger.error("Error running LogReducer test.", e)
    }
  }
}
