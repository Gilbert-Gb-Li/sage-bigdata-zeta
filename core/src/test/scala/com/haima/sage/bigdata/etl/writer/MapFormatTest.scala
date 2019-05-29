package com.haima.sage.bigdata.etl.writer

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.writer.{Syslog, SyslogOutputFormat}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Test

class MapFormatTest {

  /**
    * 测试优先级
    */
  @Test
  def syslogFormatPriority(): Unit = {
    val date=new Date()
    val format=new SimpleDateFormat("MMM dd HH:mm:ss")
    val format2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val host= InetAddress.getLocalHost.getHostAddress
    // 情况一：优先级引用数据字段，引用字段在数据中存在
    val syslogA:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("a"),timestamp=Some("@timestamp")))
    val rtA=SyslogFormat(syslogA).string(Map("a"->"12","@timestamp"->format2.format(date)))
    val dataA=s"""<12>${format.format(date)} $host {"a":"12","@timestamp":"${format2.format(date)}"}""".stripMargin
    assert(dataA.equals(rtA) )
    //情况二：优先级引用数据字段，引用字段在数据中不存在
    val syslogB:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp")))
    val rtB=SyslogFormat(syslogB).string(Map("a"->"12","@timestamp"->format2.format(date)))
    val dataB=s"""<0>${format.format(date)} $host {"a":"12","@timestamp":"${format2.format(date)}"}""".stripMargin
    assert(dataB.equals(rtB) )
    //情况三：优先级固定值
    val syslogC:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("VAL"),
      facility = Option("1"),level = Option("2"),timestamp=Some("@timestamp")))
    val rtC=SyslogFormat(syslogC).string(Map("a"->"12","@timestamp"->format2.format(date)))
    val dataC=s"""<10>${format.format(date)} $host {"a":"12","@timestamp":"${format2.format(date)}"}""".stripMargin
    assert(dataC.equals(rtC) )
    //情况四：优先级不设置
    val syslogD:Syslog =Syslog(syslogFormat=com.haima.sage.bigdata.etl.common.model.writer.SyslogOutputFormat(priorityType=Some("NULL"),
      timestamp=Some("@timestamp")))
    val rtD=SyslogFormat(syslogD).string(Map("a"->"12","@timestamp"->format2.format(date)))
    val dataD=s"""<0>${format.format(date)} $host {"a":"12","@timestamp":"${format2.format(date)}"}""".stripMargin
    assert(dataD.equals(rtD) )

  }

  /**
    * 测试主机
    */
  @Test
  def syslogFormatHostname(): Unit = {
    val date=new Date()
    val format=new SimpleDateFormat("MMM dd HH:mm:ss")
    val format2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //情况一：主机引用数据字段，引用字段在数据中存在
    val syslogA:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp"),hostname = Some("$hostname")))
    val rtA=SyslogFormat(syslogA).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test"))
    val dataA=s"""<0>${format.format(date)} test {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test"}""".stripMargin
    assert(dataA.equals(rtA) )
    //情况二：主机引用数据字段，引用字段在数据中不存在
    val hostB= InetAddress.getLocalHost.getHostAddress
    val syslogB:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp"),hostname = Some("$host")))
    val rtB=SyslogFormat(syslogB).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test"))
    val dataB=s"""<0>${format.format(date)} $hostB {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test"}""".stripMargin
    assert(dataB.equals(rtB) )

    //情况三：主机固定值
    val syslogC:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp"),hostname = Some("10.20.50.100")))
    val rtC=SyslogFormat(syslogC).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test"))
    val dataC=s"""<0>${format.format(date)} 10.20.50.100 {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test"}""".stripMargin
    assert(dataC.equals(rtC) )

    //情况四：主机字段不配置
    val hostD= InetAddress.getLocalHost.getHostAddress
    val syslogD:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp"),hostname = Some("$host")))
    val rtD=SyslogFormat(syslogD).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test"))
    val dataD=s"""<0>${format.format(date)} $hostD {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test"}""".stripMargin
    assert(dataD.equals(rtD) )
  }

  /**
    * 测试tag
    */
  @Test
  def syslogFormatTag(): Unit = {
    val date=new Date()
    val format=new SimpleDateFormat("MMM dd HH:mm:ss")
    val format2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //情况一：TAG引用数据字段，引用字段在数据中存在
    val syslogA:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp"),hostname = Some("$hostname"),tag = Some("$tag")))
    val rtA=SyslogFormat(syslogA).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test","tag"->"sshd"))
    val dataA=s"""<0>${format.format(date)} test sshd {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test","tag":"sshd"}""".stripMargin
    assert(dataA.equals(rtA) )
    //情况二：TAG引用数据字段，引用字段在数据中不存在
    val hostB= InetAddress.getLocalHost.getHostAddress
    val syslogB:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp"),hostname = Some("$host") ,tag = Some("$tag")))
    val rtB=SyslogFormat(syslogB).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test"))
    val dataB=s"""<0>${format.format(date)} $hostB {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test"}""".stripMargin
    assert(dataB.equals(rtB) )

    //情况三：主机固定值
    val syslogC:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp"),hostname = Some("10.20.50.100"),tag = Some("mysqld")))
    val rtC=SyslogFormat(syslogC).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test"))
    val dataC=s"""<0>${format.format(date)} 10.20.50.100 mysqld {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test"}""".stripMargin
    assert(dataC.equals(rtC) )

    //情况四：主机字段不配置
    val hostD= InetAddress.getLocalHost.getHostAddress
    val syslogD:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestamp=Some("@timestamp"),hostname = Some("$host"),tag = None))
    val rtD=SyslogFormat(syslogD).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test"))
    val dataD=s"""<0>${format.format(date)} $hostD {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test"}""".stripMargin
    assert(dataD.equals(rtD) )
  }

  /**
    * 测试时间
    */
  @Test
  def syslogFormatTimestamp(): Unit = {

    val date=new Date()
    val format=new SimpleDateFormat("MMM dd HH:mm:ss")
    val format2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //情况一：timestamp引用数据字段，引用字段在数据中存在
    val syslogA:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestampType=Some("DATA"),timestamp=Some("@timestamp"),hostname = Some("$hostname"),tag = Some("$tag")))
    val rtA=SyslogFormat(syslogA).string(Map("a"->"12","@timestamp"->"2018-01-11 10:22:40","hostname"->"test","tag"->"sshd"))
    val dataA=s"""<0>${format.format(format2.parse("2018-01-11 10:22:40"))} test sshd {"a":"12","@timestamp":"2018-01-11 10:22:40","hostname":"test","tag":"sshd"}""".stripMargin
    assert(dataA.equals(rtA) )

    //情况二：主机固定值
    val syslogB:Syslog =Syslog(syslogFormat=SyslogOutputFormat(priorityType=Some("DATA"),
      priority = Some("b"),timestampType=Some("VAL"),timestamp=Some("2018-01-11 10:22:56"),hostname = Some("10.20.50.100"),tag = Some("mysqld")))
    val rtB=SyslogFormat(syslogB).string(Map("a"->"12","@timestamp"->format2.format(date),"hostname"->"test"))
    val dataB=s"""<0>一月 11 10:22:56 10.20.50.100 mysqld {"a":"12","@timestamp":"${format2.format(date)}","hostname":"test"}""".stripMargin
    assert(dataB.equals(rtB) )
  }
}
