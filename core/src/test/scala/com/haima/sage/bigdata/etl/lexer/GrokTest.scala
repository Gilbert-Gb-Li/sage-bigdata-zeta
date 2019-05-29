package com.haima.sage.bigdata.etl.lexer

import com.haima.sage.bigdata.etl.common.model.Regex
import io.thekraken.grok.api.Grok
import org.junit.Test

/**
  * Created by zhhuiyan on 15/3/25.
  */
class GrokTest {

  /*@Test
  def 国民银行(): Unit = {
    val data = "<13>Dec 15 13:49:54 kibapoc1 ...168.194\"/><Fld id=\"lang\" value=\"ZH\"/><Fld id=\"loopcnt\" value=\"1\"/><Fld id=\"masterid\" value=\"GAO021\"/><Fld id=\"message\" value=\"Success\"/><Fld id=\"nation\" value=\"CN\"/><Fld id=\"secretNo\" value=\"\"/><Fld id=\"skipcnt\" value=\"1\"/><Fld id=\"svrid\" value=\"20171215001633\"/><Fld id=\"trandt\" value=\"20171215134858\"/><Fld id=\"txcode\" value=\"KFIICB7441\"/><Fld id=\"type\" value=\"0\"/><Fld id=\"wholecnt\" value=\"1\"/></IntegrationHeader></IntegrationHeaders><TranHeader><Fld id=\"Acno\" value=\"\"/><Fld id=\"ChipNo\" value=\"\"/><Fld id=\"CrdtCardNo\" value=\"\"/><Fld id=\"CuniqNo\" value=\"\"/><Fld id=\"Reserved1\" value=\"\"/><Fld id=\"Reserved2\" value=\"\"/><Fld id=\"Ssno\" value=\"\"/><Fld id=\"UID\" value=\"\"/><Fld id=\"UserPwd\" value=\"\"/></TranHeader><MsgBody TaskCount=\"1\" wfrefCols=\"\" wfrefID=\"\"><Task id=\"\" number=\"1\" rtcode=\"\"><Device clear=\"N\" formid=\"KFIICB74000\" msg=\"\" type=\"screen\"><Fld id=\"bzwkClsfiCd\" value=\"B\"/><Fld id=\"bzwkCommonAccountingShowYN\" value=\"N\"/><Fld id=\"bzwkCommonBrncoBrncd\" value=\"9000\"/><Fld id=\"bzw..."
    val grok = Grok.getInstance("<%{GREEDYDATA:syslog_pri}>%{GREEDYDATA:month}\\s+%{GREEDYDATA:day}\\s+%{GREEDYDATA:time}\\s+%{GREEDYDATA:hostname}\\s+%{GREEDYDATA}\\s+%{GREEDYDATA:log_path}\\s+%{GREEDYDATA:log_type}")
    val matched = grok.`match`(data)
    matched.captures(true)
    println(matched.toMap)


    """<(\d+)>(\S{3})\s(\d{2})\s(\d{2}:\d{2}:\d{2})\s(\S*)\s.*\s+(.*)\s+(.*)\\|""".r("syslog_pri", "month", "day", "time", "hostname", "log_path", "message").findFirstMatchIn(data) match {
      case Some(m) =>
        println(m.groupNames.map(name => (name, m.group(name))).toMap)
      case None =>
    }


  }*/

   @Test
  def ias(): Unit ={
     val grok=Grok.getInstance("%{DATA:date} %{DATA:time} \\[%{DATA:thread}\\] \\[%{WORD:level}\\] \\[%{DATA:type}\\] %{DATA}:%{ALL:data}")
     val data="""2018-08-24 11:00:00.339 [sage-bigdata-collector,3e02b6e81f513e02,8ae5ac6dc0945408,qtp917156101-219497] [DEBUG] [ias-request] 请求参数:{"timestamp":1535079631616,"data":[{"search_id":"孔小智","live_id":"7928","user_id":"7928","data_generate_time":1535079571613,"message_info":[{"audience_name":"宇宙无敌大拜仁","audience_id":"15328304"}]},{"search_id":"孔小智","live_id":"7928","user_id":"7928","data_generate_time":1535079578972,"message_info":[{"audience_name":"宇宙无敌大拜仁","audience_id":"15328304","content":"我来也"}]},{"search_id":"孔小智","live_id":"7928","user_id":"7928","data_generate_time":1535079598484,"message_info":[{"audience_name":"宇宙无敌大拜仁","audience_id":"15328304","content":"我旅行回来了"}]}],"schema":"live_danmu","protocol_version":"2.0.0","spider_version":"2.0.0","hsn_id":"5d421247ad4a14b2d7397df7f86cd5b16c3a12cc","app_package_name":"com.huomaotv.mobile","app_version":"2.3","template_version":"190"}
                """.stripMargin
     val matched = grok.`match`(data)
     matched.captures(true)
     println(matched.toMap)
   }
  @Test
  def testDouble(): Unit = {
    val latitude: String = "1231"
    val _latitude = latitude.trim match {
      case null =>
        0
      case l: String if l.matches("-?\\d*\\.?\\d+") =>
        l.toDouble
      case _ =>
        0
    }
    assert(_latitude == 1231, "must be ")
  }


  @Test
  def testDataTime(): Unit = {
    val grok = RegexLexer(Regex(s"%{SYSLOGTIMESTAMP:@timestamp}"))
    //println(TimestampMaker.apply().suffix(grok.parse("Oct  2 12:52:00")))

  }

  @Test
  def test(): Unit = {
    val grok = RegexLexer(Regex("""id=%{WORD:id} time="%{DATA:logdate}" fw=%{DATA:fw}\bpri=%{DATA:pri} type=%{DATA:type} user=%{DATA:user} src=%{IP:src} op="%{DATA:op}" result=%{DATA:result} recorder=%{DATA:recorder} msg="%{DATA:msg}""""))
    println(grok.parse("""id=tos time="2015-03-25 14:36:50" fw=中文 pri=6 type=system user=superman src=127.0.0.1 op="logout" result=0 recorder=AUTH msg=""""").toString())

  }

  @Test
  def testSyslogAlertForwarder(): Unit = {
    //"""%{NOTSPACE:alert_process}: %{DATA:server_name} %{DATA:alert_cat}: %{DATA:msg} ?(severity = %{DATA:severity}).? ?%{IP:src}:%{DATA:sport} -> %{IP:dst}:%{DATA:dport} (result = %{DATA:result})""""
    val grok = RegexLexer(Regex("""%{NOTSPACE:alert_process}: %{DATA:server_name} %{DATA:alert_cat}: %{DATA:msg} ?\(severity = %{DATA:severity}\)\.? ?%{IP:src}:%{DATA:sport} -> %{IP:dst}:%{DATA:dport} ?\(result = %{DATA:result}\)"""))
    println(grok.parse("""SyslogAlertForwarder: M3050 detected Unknown attack HTTP: Microsoft XML Core Services Remote Code Execution (severity = Medium). 168.168.17.248:N/A -> 10.228.0.1:N/A (result = Inconclusive)""").toString())

  }

  @Test
  def testRuiXing(): Unit = {
    val grok = RegexLexer(Regex("""%{NOTSPACE:computer} %{NOTSPACE}?:\s*%{NOTSPACE:event_type}\n%{NOTSPACE}?:\s*%{NOTSPACE:event_level}\n%{NOTSPACE}?:\s*%{DATA:event_info}\n%{NOTSPACE}?:\s*%{NOTSPACE:computer_name}\n%{NOTSPACE}?:\s*%{IP:ipaddress}\n%{NOTSPACE}:\s*%{NOTSPACE:reportor}\n%{NOTSPACE}?:\s*%{DATE:date} %{TIME:time}\n\s*"""))
    val grok2 = RegexLexer(Regex("""%{NOTSPACE:computer} %{NOTSPACE}?:\s*%{NOTSPACE:virus_name}\n%{NOTSPACE}?:\s*%{NOTSPACE:virus_type}\n%{NOTSPACE}?:\s*%{NOTSPACE:opt_result}\n%{NOTSPACE}?:\s*%{NOTSPACE:virus_reportor}\n%{NOTSPACE}:%{DATA:infected_file}?\n%{NOTSPACE}:\s*%{DATA:infected_path}?\n%{NOTSPACE}?:\s*%{DATE:date} %{TIME:time}\n%{NOTSPACE}?:\s*%{NOTSPACE:computer_name}\n%{NOTSPACE}?:\s*%{IP:ip}\n\s*"""))
    // val grok2=  RegexLexer(Regex("""%{NOTSPACE:computer}\n%{NOTSPACE}?:%{NOTSPACE:virus_name} %{NOTSPACE}?:\s*%{NOTSPACE:virus_type} %{NOTSPACE}?:\s*%{NOTSPACE:opt_result} %{NOTSPACE}?:\s*%{NOTSPACE:virus_reportor} %{NOTSPACE}:\s*%{DATA:infected_file}? %{NOTSPACE}:\s*%{DATA:infected_path}? %{NOTSPACE}?:\s*%{DATE:date} %{TIME:time} %{NOTSPACE}?:\s*%{NOTSPACE:computer_name} %{NOTSPACE}?:\s*%{IP:ip}\s*""",None))
    //val grok2=  RegexLexer(Regex("""%{NOTSPACE:virus_name} %{NOTSPACE}?:\s*%{NOTSPACE:virus_type} %{NOTSPACE}?:\s*%{NOTSPACE:opt_result} %{NOTSPACE}?:\s*%{NOTSPACE:virus_reportor} %{NOTSPACE}:\s*%{DATA:infected_file}? %{NOTSPACE}:\s*%{DATA:infected_path}? %{NOTSPACE}?:\s*%{TIMESTAMP:@timestamp} %{NOTSPACE}?:\s*%{NOTSPACE:computer_name} %{NOTSPACE}?:\s*%{IP:ip} """,None))
    println(grok2.parse(
      """ user-PC [病毒日志]病毒名:Worm.Win32.Gamarue.n
        |病毒类型:蠕虫
        |清除结果: 已清除
        |病毒发现者: 实时监控
        |感染的文件:C:\USERS\USER\DESKTOP\U盘病毒样本\新建文件夹 (2)\~$WQIXOGHK.FAT
        |感染路径: C:\PROGRAM FILES\WINRAR\WINRAR.EXE
        |发现日期: 2015-05-07 16:10:57
        |计算机名称: user-PC
        |IP: 156.132.88.10
        | """.stripMargin).toString())
    val map = grok.parse("user-PC [事件日志]事件类型: 扫描病毒\n事件级别: 提示信息\n事件信息: 1：开始时间2015-05-08 14:34:39，结束时间2015-05-08 15:19:38，扫描文件194922个，发现染毒病毒文件24个\n计算机名称: user-PC\nIP: 10.88.132.156\n报告者:RavService\n发现日期: 2015-05-08 15:19:38\n ")
    println(map.toString())
    //println(TimestampMaker().suffix(map))
  }


}
