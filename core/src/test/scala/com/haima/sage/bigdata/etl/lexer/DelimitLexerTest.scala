package com.haima.sage.bigdata.etl.lexer

import java.nio.file.Paths

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.filter.Script
import com.haima.sage.bigdata.etl.common.model.{ReadPosition, Regex}
import com.haima.sage.bigdata.etl.lexer.filter.ScriptProcessor
import com.haima.sage.bigdata.etl.monitor.file.LocalFileWrapper
import com.haima.sage.bigdata.etl.reader.TXTFileLogReader
import org.junit.Test

class DelimitLexerTest {
  @Test
  def delimitVPN(): Unit = {
    val path = "data/localhost_access_log.2015-11-07.txt"
    val reader = new TXTFileLogReader(path,
      None,
      None,
      LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))
    val data =
      """nsisg1000: NetScreen device_id=0133022009000047 [Root]system-notification-00257(traffic): start_time="2016-06-16 11:45:42" duration=5 policy_id=618 service=icmp proto=1 src zone=Trust dst zone=Untrust action=Permit sent=78 rcvd=78 src=192.168.165.33 dst=220.181.111.188 icmp type=0 icmp code=0 src-xlated ip=192.168.165.33 dst-xlated ip=220.181.111.188 session_id=468818 reason=Close - RESP""".stripMargin

    val regex = Regex("%{NOTSPACE:app_name}: %{GREEDYDATA}: %{GREEDYDATA:some}")
    val delimit = RegexLexer(regex)

    delimit.parse(data).foreach(println)

  }


  @Test
  def netScreen(): Unit = {

    val now = System.currentTimeMillis();
    val data =
      """nsisg1000: NetScreen device_id=0133022009000047 [Root]system-notification-00257(traffic): start_time="2016-06-16 11:45:42" duration=5 policy_id=618 service=icmp proto=1 src zone=Trust dst zone=Untrust action=Permit sent=78 rcvd=78 src=192.168.165.33 dst=220.181.111.188 icmp type=0 icmp code=0 src-xlated ip=192.168.165.33 dst-xlated ip=220.181.111.188 session_id=468818 reason=Close - RESP""".stripMargin

    val regex = Regex("%{NOTSPACE:app_name}: %{GREEDYDATA}: %{GREEDYDATA:some}")
    val delimit = RegexLexer(regex)

    (0 to 1000).foreach(i => {
      delimit.parse(data) //.foreach(println)
    })


    println(s"take time ${System.currentTimeMillis() - now}")

  }

  @Test
  def sxf(): Unit = {
    val data =
      """<13>Dec 14 11:26:27 kfiisoc1  /fswaslog/kfi/jeus50/kfiisoc1/JeusServer_20130304.log jeus|*@*|at com.kbstar.ksa.monitor.adapter.ByteAdapter.sendData(ByteAdapter.java:120)
        |#next#
        |<13>Dec  4 18:26:52 kecdsoc1  /fsbbsp/tomcat_bbsp/apache-tomcat-6.0.39/logs/catalina.out tomcat|*@*|taskpool0_.pTask_Num as pTask6_151_,",
        |"message": "kecdsoc2 /fsbbsp/tomcat_bbsp/apache-tomcat-6.0.39/logs/catalina.out tomcat| taskpool0_.pTask_Num as pTask6_151_,""".stripMargin.split("#next#")

    val parser = new ScriptProcessor(Script(
      """//取解析时间为日志标识时间//取解析时间为日志标识时间

val date: Nothing = new Nothing
event.put("@timestamp", date.toLocaleDateString + " " + date.toLocaleTimeString)
val head: Nothing = event.get("head")
if (head && head != null)  { val heads: Nothing = head.split(" ")
//当日期为10~31号时
if (heads.length eq 7)  { event.put("alert_type", heads(3) + "_" + heads(6))
event.put("log_path", heads(5))
}
//当日期为1~9号时
if (heads.length eq 8)  { event.put("alert_type", heads(4) + "_" + heads(7))
event.put("log_path", heads(6))
}
event.remove("head")
}""".stripMargin))
    val now = System.currentTimeMillis()
    val delimit = Delimiter(Some("|*@*|"), 100)
    (0 to 10000000).foreach(i => {


      data.foreach(d => {
        val das = delimit.parse(d)

        parser.process(Map("head" -> das(0), "message" -> das(1)))
      }
      )
      // delimit.parse(data)//.foreach(println)

    })


    println(s"take time ${System.currentTimeMillis() - now}")


  }
}
