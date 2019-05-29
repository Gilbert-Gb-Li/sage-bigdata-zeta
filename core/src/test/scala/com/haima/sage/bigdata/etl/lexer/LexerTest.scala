package com.haima.sage.bigdata.etl.lexer

import java.io.{File, FileOutputStream}
import java.nio.file.Paths

import com.haima.sage.bigdata.etl.codec.{MultiCodec, Contain => CodecContain}
import com.haima.sage.bigdata.etl.common.QuoteString
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.common.model.{ReadPosition, _}
import com.haima.sage.bigdata.etl.monitor.file.{JavaFileUtils, LocalFileWrapper}
import com.haima.sage.bigdata.etl.reader.TXTFileLogReader
import org.junit.Test

/**
  * Created by zhhuiyan on 15/4/14.
  */
class LexerTest {
  /*
    nsisg1000: NetScreen device_id=0133022009000047 [Root]system-notification-00257(traffic): start_time="2016-06-16 11:45:42" duration=5 policy_id=618 service=icmp proto=1 src zone=Trust dst zone=Untrust action=Permit sent=78 rcvd=78 src=192.168.165.33 dst=220.181.111.188 icmp type=0 icmp code=0 src-xlated ip=192.168.165.33 dst-xlated ip=220.181.111.188 session_id=468818 reason=Close - RESP
  */


  @Test
  def json(): Unit = {

    val data =
      """{   "author":"aaa",
        |    "programmers": [{
        |        "firstName": "Brett",
        |        "lastName": "McLaughlin",
        |        "email": "aaaa"
        |    }, {
        |        "firstName": "Jason",
        |        "lastName": "Hunter",
        |        "email": "bbbb"
        |    }, {
        |        "firstName": "Elliotte",
        |        "lastName": "Harold",
        |        "email": "cccc"
        |    }],
        |    "authors": [{
        |        "firstName": "Isaac",
        |        "lastName": "Asimov",
        |        "genre": "sciencefiction"
        |    }, {
        |        "firstName": "Tad",
        |        "lastName": "Williams",
        |        "genre": "fantasy"
        |    }, {
        |        "firstName": "Frank",
        |        "lastName": "Peretti",
        |        "genre": "christianfiction"
        |    }],
        |    "musicians": [{
        |        "firstName": "Eric",
        |        "lastName": "Clapton",
        |        "instrument": "guitar"
        |    }, {
        |        "firstName": "Sergei",
        |        "lastName": "Rachmaninoff",
        |        "instrument": "piano"
        |    }]
        |}""".stripMargin


    val json = JSONLogLexer(JsonParser())

    val rt = json.parse(data)
    assert(rt.get("author").contains("aaa"), s"json parse must be contains author with value[aaa]")
    val programmers = rt("programmers")
    assert(programmers.isInstanceOf[java.util.List[_]], s"json parse must be support array as list")

    assert(programmers.asInstanceOf[java.util.List[_]].size() == 3, s"json parse must be support array as list and size is 3")
    val programmer = programmers.asInstanceOf[java.util.List[_]].get(0)
    assert(programmer.isInstanceOf[java.util.Map[_, _]], s"json parse must be support array[map]")
    assert(programmer.asInstanceOf[java.util.Map[String, Any]].get("email") == "aaaa", s"json parse programmers(0) must be contains author with value[aaaa]")
  }


  /*
  * May 06 2016 06:29:43 VFW-Site : %ASA-6-106015: Deny TCP (no connection) from 192.168.1.124/2479 to 172.17.200.134/8888 flags FIN ACK on interface outside907
%ASA-6-302016: Teardown UDP connection 1430271742 for outside:172.17.200.244/53 to Hadoop:172.27.8.36/56466 duration 0:00:00 bytes 166
*/
  @Test
  def regex(): Unit = {
    val 数据 = """May 06 2016 06:29:43 VFW-Site : %ASA-6-106015: Deny TCP (no connection) from 192.168.1.124/2479 to 172.17.200.134/8888 flags FIN ACK on interface outside907"""
    val lexer = RegexLexer(Regex(s"(%{DATE:date} %{TIME:time} %{NOTSPACE:server} : )?\\%%{NOTSPACE:thread}: %{NOTSPACE:opt} %{DATA} from %{IP:src}/%{NUMBER:src_port} to %{IP:dst}/%{NUMBER:dst_port} %{DATA:msg}%{NUMBER:outside}"))
    val rt = lexer.parse(数据)

    assert(rt.get("date").contains("May 06 2016"), "after parse must be contain  field[date] with value[May 06 2016]")
    assert(rt.get("time").contains("06:29:43"), "after parse must be contain  field[time] with value[06:29:43]")
    assert(rt.contains("server"), "after parse must be contain  field[date]")
    assert(rt.contains("opt"), "after parse must be contain  field[date]")

  }

  @Test
  def waf3(): Unit = {
    val 数据 = """%ASA-6-302016: Teardown UDP connection 1430271742 for outside:172.17.200.244/53 to Hadoop:172.27.8.36/56466 duration 0:00:00 bytes 166"""

    println(RegexLexer(Regex(s"\\%%{NOTSPACE:thread}: %{NOTSPACE:opt} UDP connection %{NUMBER:conn_id} for outside:%{IP:src}/%{NUMBER:src_port} to Hadoop:%{IP:dst}/%{NUMBER:dst_port} duration %{TIME:duration} bytes %{NUMBER:bytes}")).parse(数据))
  }

  @Test
  def waf(): Unit = {
    val 数据 = """stat_time:2016-05-06 02:03:00 cpu:2 mem:13"""

    println(RegexLexer(Regex("stat_time:%{DATE:date} %{TIME:time} cpu:%{NOTSPACE:cpu} mem:%{NOTSPACE:mem}")).parse(数据))
  }

  @Test
  def delimiterWithKeyMap(): Unit = {
    val data = "date=2015-09-09 09:00:05;src=172.16.8.24;dst=miserupdate.aliyun.com;application_type=HTTP文件下载;application=应用程序;detail=服务端口: 80 ,行为内容: 从miserupdate.aliyun.com/data/miser_app_update/17/softpacket/taobaoprotect.exe下载应用程序类的文件该操作被拒绝,策略: PV机关员工默认策略,网站: miserupdate.aliyun.com,URL: miserupdate.aliyun.com,文件类型: 应用程序,文件名: taobaoprotect.exe,DNS: miserupdate.aliyun.com;response=被拒绝"
    val parser = DelimitWithKeyMap(Some(";"), Some("="))
    val lexer = DelimiterWithKeyMapLexer(parser)
    val rt = lexer.parse(data)
    assert(rt.get("date").contains("2015-09-09 09:00:05"), "after parse must be contain  field[date] with value[2015-09-09 09:00:05]")
    assert(rt.get("src").contains("172.16.8.24"), "after parse must be contain  field[src] with value[06:29:43]")
    assert(rt.contains("dst"), "after parse must be contain  field[dst]")
    assert(rt.contains("response"), "after parse must be contain  field[response]")
  }

  @Test
  def testdelimitwaf(): Unit = {
    //    /*"""Host: www.bjgasgh.com
    //       Connection: close
    //       User-Agent: Baiduspider-image+(+http://www.baidu.com/search/spider.htm)
    //       Accept-Encoding: gzip
    //       Accept: */* alertinfo:None proxy_info:None characters:16,59,1,0,11,Baiduspider-image+(+http://www.baidu.com/search/spider.htm)||||| count_num:1 protocol_type:HTTP wci:None wsi:None"""
    //    """localhost waf: tag:waf_log_websec site_id:1428395845 protect_id:2442812971 dst_ip:172.17.100.24 dst_port:80 src_ip:123.125.71.18 src_port:56884 method:GET domain:www.bjgasgh.com uri:/info/picture/2014/2/201402121104371150.jpg alertlevel:LOW event_type:Spider_Anti stat_time:2016-05-11 11:31:46"""
    //    */
    val data =
    """policy_id:1835009 rule_id:22806528 action:Block block:No block_info:None http:GET /info/picture/2014/2/201402121104371150.jpg HTTP/1.1""".stripMargin
    val parser = DelimitWithKeyMap(Some(" "), Some(":"))
    val lexer =DelimiterWithKeyMapLexer(parser)

    val rt = lexer.parse(data)
    assert(rt.get("policy_id").contains("1835009"), "after parse must be contain  field[policy_id] with value[1835009]")
    assert(rt.get("action").contains("Block"), "after parse must be contain  field[action] with value[Block]")
    assert(rt.contains("http"), "after parse must be contain  field[dst]")


  }

  /*
  **/


  @Test
  def chinaMobile() {
    val parser = Regex("%{_MONTH_DAY}-%{_MONTH_NUM}\\u6708 -%{_YEAR} %{_HOUR}\\.%{_MINUTE}\\.%{_SECOND}\\.%{_MCRO_SECOND} (\\u4e0b|\\u4e0a)\\u5348"
    )
    println(RegexLexer(parser).parse("01-9月 -15 11.00.07.0000000 下午"))
  }

  /*
  * %{TOMCAT}*/
//  @Test
//  def tomcat(): Unit = {
//    val path = "data/localhost_access_log.2015-11-07.txt"
//
//    val reader = new TXTFileLogReader(path, None, None, LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))
//
//
//    val parser = Regex("%{IPORHOST:c_ip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:@timestamp}\\] \\\"(?:%{WORD:method} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})\\\" %{NUMBER:response} (?:%{NUMBER:bytes}|-)( %{QS:referrer} %{QS:agent}( %{NUMBER:read} %{NUMBER:write})?)?"
//    )
//
//    val lexer = RegexLexer(parser)
//    /*TOMCAT %{IPORHOST:c_ip} - %{USERNAME:remote_user} \[%{HTTPDATE:@timestamp}\] %{QS:request} %{INT:status} (%{INT:body_bytes_sent}|-) url_refer*/
//    lexer.
//      parse("192.168.190.97 - - [07/Nov/2015:10:21:16 +0800] \"GET /resources/extjs/resources/images/default/tree/elbow-end-minus.gif HTTP/1.1\" 304 - \"http://portal.hbcf.edu.cn/main\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0)\"")
//      .foreach(println)
//    /*reader.foreach(event => {
//      val parsed=lexer.parse(event.content)
//     /* println(parsed)
//      println(event.content)
//      println()*/
//      if(parsed.contains("referrer")){
//        println(event.content)
//        println(parsed)
//      }
//
//
//    })*/
//  }

  @Test
  def amrt(): Unit = {
    val path1 = try {
      Paths.get("data/amrt.log")
    } catch {
      case e: Exception =>
        null
    }

    val localFile = new LocalFileWrapper(path1, Some("UTF-8"))
    val stream = localFile.stream

    //  val lineSeparator: String = JavaFileUtils.lineSeparator(stream)

    // println("---" + lineSeparator)
    val path = "data/amrt.log"

    val reader = new TXTFileLogReader(path, None, None, LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))


    val filter = Filter(Array(

      MapMatch("action", Array(
        MapCase("SESSION", ReParser(Some("msg"), DelimitWithKeyMap(delimit = Some(" ")))),
        MapCase("PNB19", ReParser(Some("msg"), Delimit(delimit = Some(" "), fields = Array("type", "msg"), filter = Array(
          MapMatch("type", Array(
            MapCase("dnsquery", ReParser(Some("msg"), Delimit(delimit = Some(" "), fields = Array("time", "url", "dst_ip", "dns")))),
            MapCase("www", ReParser(Some("msg"), Delimit(delimit = Some(" "), fields = Array("time", "src_ip", "method", "dst_host", "dst_url"))))

          ))
        ))))

      ))))

    val parser = Regex("%{SYSLOGTIMESTAMP:@timestamp} %{NOTSPACE:host_name} %{NOTSPACE:action}(: |>)%{DATA:msg}"
      , filter.rules)

    val lexer = RegexLexer(parser)
    reader.foreach(event => {
      val parsed = lexer.parse(event.content)

      if (parsed.contains("error")) {
        println()
        println(parsed)
        println(event.content)
      } else {
        val result = filter.filter(parsed)
        if (result.contains("error")) {
          println()
          println(parsed)
          println(event.content)
        }
      }

    })
  }


  /*@Test
  def commerce(): Unit = {

    val path = "data/commerce.log"

    val reader = new TXTFileLogReader(path, None, LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))

    val parser = Regex("%{SYSLOGTIMESTAMP:@timestamp} %{NOTSPACE:from} %{DATA:type}: %{ALL:logs}")
    reader.foreach(event => {
      println(TimestampMaker.create().suffix(RegexLexer(parser).parse(event.content)))


    })
  }*/

  @Test
  def linux_secure(): Unit = {

    val path = "data/secure.log"

    val reader = new TXTFileLogReader(path, None, None, LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))

    val parser = Regex("%{SYSLOGTIMESTAMP:@timestamp} %{WORD:computer} %{WORD:action}\\[%{NUMBER:process}\\]:%{ALL:seg} ")
    reader.foreach(event => {
      println(RegexLexer(parser).parse(event.content))
    })
  }

  @Test
  def delimit(): Unit = {

    (0 until 1).foreach(println)
    println(DelimiterLexer(Delimit(fields = Array("a", "b", "c", "d", "e"))).parse("206703762,\"2012120,薛晶晶\",\"2006\"\"CCTV杯\"\"全国英语演讲大赛(附光盘两张)\",借,2015/10/8 7:51"))
  }

  @Test
  def macths(): Unit = {
    val regex = "^\\[.*?\\]\\s\\w+\\s\\w+\\s+\\w\\s\\w+".r
    val n = "[15-9-15 16:40:54:781 CST] 00000029 SystemErr     R \tat com.ibm.ws.util.ThreadPool$Worker.run(ThreadPool.java:1473)"
    regex.findFirstIn("[15-9-15 16:41:26:244 CST] 00000032 SystemErr     R com.ibm.websphere.ce.cm.ObjectClosedException: DSRA9110E: 关闭 ResultSet。").foreach(println)
    regex.findFirstIn(n).foreach(println)
  }

  @Test
  def split(): Unit = {
    "Errors in file /u01/oracle/product/diag/rdbms/avcon/avcon/trace/avcon_j000_26147.trc:".replaceAll(" ", "\t \t").split(" ").foreach(data => println(data.trim()))

    DelimiterLexer(Delimit(Some(" "), Array("a", "b"))).parse("Errors in file /u01/oracle/product/diag/rdbms/avcon/avcon/trace/avcon_j000_26147.trc:").foreach(println)
    //   "Errors in file /u01/oracle/product/diag/rdbms/avcon/avcon/trace/avcon_j000_26147.trc:".split(" ").foreach(println)
  }

//  @Test
//  def sxf_parser(): Unit = {
//
//    val path = "data/cmb.pro/nbwebpb73.User20150623.txt"
//
//    val reader = new TXTFileLogReader(path, None, None, LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))
//
//    val out = new FileOutputStream(new File("data/cmb.pro/nbwebpb73.User20150623.json.txt"))
//
//    reader.foreach(event => {
//      /*
//      * "LOG":"20150827 14:57:28  <Root><Even><AID></AID><SID></SID><OperateCode>T815201</OperateCode><TransCode>T815201</TransCode><SessionID>E68A368F8A489DB1FB9668C3B6394AC2122403022550287300105752</SessionID><ClientNo>E68A368F8A489DB1FB9668C3B6394AC2122403022550287300105752</ClientNo><URL>/USER/UNIONUSERREGISTER/REG_SETPWD.ASPX</URL><AccType>0</AccType><AccNo>13535358282</AccNo><CurrencyType>M</CurrencyType><Amount></Amount><Share></Share><Fee></Fee><OpponentAccNo></OpponentAccNo><CommitTime>20150827 14:57:28</CommitTime><ResultCode>Y</ResultCode><ErrorCode></ErrorCode><ErrorMsg></ErrorMsg><Remark></Remark><CustomerIP>125.95.129.56</CustomerIP><CustomerMAC>000000000842015073051A53462-85A8-4E5D-B91A-7C47C214AEDC0GhY1NR0=</CustomerMAC><GUID>5746587459709709268</GUID><NetBankCookieID></NetBankCookieID><LoginIDType>A</LoginIDType><LoginID>6214860207472347</LoginID><DeviceType>D</DeviceType><IDType>P01</IDType><IDCard>440401196806272118</IDCard><Name>?????/Name><Location></Location></Even></Root>\r","TOPIC":"mobilebank","FILE_PATH":"D:\\Applications\\MobileDataRoot\\Mobile_4_0\\SysLog\\html\\UserLog20150827.txt"}*/
//      /*  event.content.split("\t").foreach(println)*/
//
//      out.write(s"""{"SOURCE_TYPE":"MobileBank_IIS","FILE_NAME":"User20150623.txt","SOURCE_HOST":"Mobile-YZH","AGENT_TIMESTAMP":"1440658649339","TOPIC":"mobilebank_iis","FILE_PATH":"data/cmb.pro/nbwebpb73.User20150623.json.txt","LOG":"${event.content}"}""".getBytes)
//      out.write("\n".getBytes)
//    })
//    out.flush()
//    out.close()
//
//
//  }


//  @Test
//  def multi_parser(): Unit = {
//
//
//    val path = "data/cmb.pro/nbwebpb73.User20150623.txt"
//
//    val reader = new TXTFileLogReader(path,
//      Some(MultiCodec("\\d+[,]\\d+", Some(true), None)), None,
//      LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))
//
//
//    val out = new FileOutputStream(new File("data/视频会议管理was及数据库日志/alert_avcon_merge.log"))
//
//    reader.slice(0, 100).foreach(event => {
//      println(event.content)
//    })
//    out.flush()
//    out.close()
//
//
//  }

//  @Test
//  def avcon_parser(): Unit = {
//
//    val path = "data/av_log/alert_avcon.log"
//    val reader = new TXTFileLogReader(path,
//      Some(MultiCodec("\\w{3}\\s+\\w{3}\\s+\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}\\s+\\d{4}", Some(true), None)), None,
//      LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))
//
//
//    val out = new FileOutputStream(new File("data/视频会议管理was及数据库日志/alert_avcon_merge.log"))
//
//    reader.slice(0, 100).foreach(event => {
//      val map =
//        Filter(Array(AddFields(Map("bak" -> "%{LOG}")), ReParser(Some("bak"), Delimit(Some(" "), Array("opt", "_igore"))), MapStartWith("LOG", Array(
//          MapCase("Errors", ReParser(Some("LOG"), Delimit(Some("\t"), Array("msg", "info"), Array(ReParser(Some("info"), DelimitWithKeyMap(Some("\t"), Some(": "))))))),
//          MapCase("Thread", ReParser(Some("LOG"), Delimit(Some("\t"), Array("msg", "info")))))))).filter(RegexLexer(Regex(s"%{DATESTAMP:@timestamp}\t%{ALL:LOG}")).parse(event.content))
//      println(map)
//      // out.write((map.mkString(",")+"\n").getBytes)
//    })
//    out.flush()
//    out.close()
//
//
//  }

  @Test
  def avcon_std_out(): Unit = {

    val path = "data/视频会议管理was及数据库日志/SystemOut.log"
    val reader = new TXTFileLogReader(path, Some(MultiCodec("\\[", Some(true), None)), None, LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))

    val filter = Filter(Array(MapContain("msg", Array(
      MapCase("====:", ReParser(Some("msg"),
        Delimit(Some("====:"), Array("msg", "xml"), Array(ReParser(Some("xml"), XmlParser())))
      )))),
      MapStartWith("msg", Array(MapCase("<?xml", ReParser(Some("msg"), XmlParser()))))
    ))
    val parser = Regex("[\\[]%{GREEDYDATA:@timestamp}[\\]][\\s]+%{NOTSPACE:id}[\\s]+%{NOTSPACE:appname}[\\s]+%{NOTSPACE:cmd}[\\s]+%{NOTSPACE:time}[\\s]+%{NOTSPACE:level}[\\s]+[\\[]%{GREEDYDATA:subsys}[\\]][\\s]+%{ALL:msg}",
      Array(MapContain("msg", Array(
        MapCase("====:", ReParser(Some("msg"),
          Delimit(Some("====:"), Array("msg", "xml"), Array(ReParser(Some("xml"), XmlParser())))
        )))),
        MapStartWith("msg", Array(MapCase("<?xml", ReParser(Some("msg"), XmlParser()))))
      ))
    reader.foreach(event => {
      println(filter.filter(RegexLexer(parser).parse(event.content)))
    })
  }

  @Test
  def avcon_std_error(): Unit = {


    val path = "data/av_log/SystemErr.log"
    val reader = new TXTFileLogReader(path, Some(MultiCodec("\\[.*?\\]\\s\\w+\\s\\w+\\s+\\w\\s\\w+", Some(true), None)), None, LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))


    /* val filter = Filter(Array(Contain("msg", Array(
       Case("====:", Analyzer(Some("msg"),
         Delimit(Some("====:"), Array("msg", "xml"), Some(Filter(Array(Analyzer(Some("xml"), XmlParser())))))
       )))),
       StartWith("msg", Array(Case("<?xml", Analyzer(Some("msg"), XmlParser()))))
     ))*/
    val fileter = (Filter(Array(MapMatch("msg", Array(MapCase("^\\d+[\\s\\S]*", ReParser(Some("msg"), Delimit(Some(" "), Array("_date", "_time", "msg")))))), ReParser(Some("msg"), Delimit(Some("\t"), Array("msg", "Info"))))))
    val parser = Regex("\\[%{DATESTAMP:@timestamp}\\]\\s+%{NOTSPACE:id}\\s+%{NOTSPACE:appname}\\s+%{NOTSPACE:cmd}\\s+%{ALL:msg}",
      Array(MapMatch("msg", Array(MapCase("^\\d+[\\s\\S]*", ReParser(Some("msg"), Delimit(Some(" "), Array("_date", "_time", "msg")))))), ReParser(Some("msg"), Delimit(Some("\t"), Array("msg", "Info")))))
    reader.foreach(event => {

      if (event != null) {
        println(fileter.filter(RegexLexer(parser).parse(event.content)))
      }

      // println(filter.filter(RegexLexer(parser).parse(event.content)))
    })
  }

  @Test
  def jsonTest(): Unit = {
    val map = Map("key" -> "a\ra", "b" -> "b\nb")
    map.foreach(println)
    println(map)

    //System.out.println("sdasdadasdasd\rasdasdasdas\nsadasdsa")
    // println("sdasdadasdasd\rasdasdasdas\nsadasdsa")
    System.out.println("hello world \r rafter \n nafter \f fafter");

    val data = """{"SOURCE_TYPE":"MobileBanck_User","FILE_NAME":"UserLog20150827.txt","SOURCE_HOST":"Mobile-SZG","AGENT_TIMESTAMP":"1440658649339","LOG":"20150827 14:57:28  <Root><Even><AID></AID><SID></SID><OperateCode>T815201</OperateCode><TransCode>T815201</TransCode><SessionID>E68A368F8A489DB1FB9668C3B6394AC2122403022550287300105752</SessionID><ClientNo>E68A368F8A489DB1FB9668C3B6394AC2122403022550287300105752</ClientNo><URL>/USER/UNIONUSERREGISTER/REG_SETPWD.ASPX</URL><AccType>0</AccType><AccNo>13535358282</AccNo><CurrencyType>M</CurrencyType><Amount></Amount><Share></Share><Fee></Fee><OpponentAccNo></OpponentAccNo><CommitTime>20150827 14:57:28</CommitTime><ResultCode>Y</ResultCode><ErrorCode></ErrorCode><ErrorMsg></ErrorMsg><Remark></Remark><CustomerIP>125.95.129.56</CustomerIP><CustomerMAC>000000000842015073051A53462-85A8-4E5D-B91A-7C47C214AEDC0GhY1NR0=</CustomerMAC><GUID>5746587459709709268</GUID><NetBankCookieID></NetBankCookieID><LoginIDType>A</LoginIDType><LoginID>6214860207472347</LoginID><DeviceType>D</DeviceType><IDType>P01</IDType><IDCard>440401196806272118</IDCard><Name>?????/Name><Location></Location></Even></Root>\r","TOPIC":"mobilebank","FILE_PATH":"D:\\Applications\\MobileDataRoot\\Mobile_4_0\\SysLog\\html\\UserLog20150827.txt"}""".stripMargin
    val data2 = """{"SOURCE_TYPE":"MobileBanck_User","FILE_NAME":"UserLog20150827.txt","SOURCE_HOST":"Mobile-SZG","AGENT_TIMESTAMP":"1440658649339","LOG":"20150827 14:57:28  <Root><Even><AID></AID><SID></SID><OperateCode>Q815238</OperateCode><TransCode>Q815238</TransCode><SessionID>E68A368F8A489DB1FB9668C3B6394AC2577517994941957900106566</SessionID><ClientNo>E68A368F8A489DB1FB9668C3B6394AC2577517994941957900106566</ClientNo><URL>/LOGIN/CLIENT/LOGINCLIENT.ASPX</URL><AccType>0</AccType><AccNo>410222198912203524</AccNo><CurrencyType>01</CurrencyType><Amount></Amount><Share></Share><Fee></Fee><OpponentAccNo></OpponentAccNo><CommitTime>20150827 14:57:28</CommitTime><ResultCode>Y</ResultCode><ErrorCode></ErrorCode><ErrorMsg></ErrorMsg><Remark></Remark><CustomerIP>112.97.55.9</CustomerIP><CustomerMAC>00000000084201508011B62A780-BBA3-4E80-A3E5-3ABB1165C2100A4BCVZg=</CustomerMAC><GUID>5422714215687510810</GUID><NetBankCookieID></NetBankCookieID><LoginIDType>A</LoginIDType><LoginID>6214856555267396</LoginID><DeviceType>D</DeviceType><IDType>P01</IDType><IDCard>410222198912203524</IDCard><Name>?????/Name><Location>35.813768|108.017137</Location></Even></Root>\r","TOPIC":"mobilebank","FILE_PATH":"D:\\Applications\\MobileDataRoot\\Mobile_4_0\\SysLog\\html\\UserLog20150827.txt"}""".stripMargin

    val data3 = """20150827 14:57:28  <Root><Even><AID></AID><SID></SID><OperateCode>T815201</OperateCode><TransCode>T815201</TransCode><SessionID>E68A368F8A489DB1FB9668C3B6394AC2122403022550287300105752</SessionID><ClientNo>E68A368F8A489DB1FB9668C3B6394AC2122403022550287300105752</ClientNo><URL>/USER/UNIONUSERREGISTER/REG_SETPWD.ASPX</URL><AccType>0</AccType><AccNo>13535358282</AccNo><CurrencyType>M</CurrencyType><Amount></Amount><Share></Share><Fee></Fee><OpponentAccNo></OpponentAccNo><CommitTime>20150827 14:57:28</CommitTime><ResultCode>Y</ResultCode><ErrorCode></ErrorCode><ErrorMsg></ErrorMsg><Remark></Remark><CustomerIP>125.95.129.56</CustomerIP><CustomerMAC>000000000842015073051A53462-85A8-4E5D-B91A-7C47C214AEDC0GhY1NR0=</CustomerMAC><GUID>5746587459709709268</GUID><NetBankCookieID></NetBankCookieID><LoginIDType>A</LoginIDType><LoginID>6214860207472347</LoginID><DeviceType>D</DeviceType><IDType>P01</IDType><IDCard>440401196806272118</IDCard><Name>?????/Name><Location></Location></Even></Root>\r""".stripMargin
    data3.split(" ", 3).foreach {
      d =>
        println(d.trim)
    }
    println(Filter(Array(ReParser(Some("LOG"), Delimit(Some(" "), Array("date", "time", "msg"))), ReParser(Some("msg"), XmlParser()))).filter(JSONLogLexer().parse(data2)))
    println(JSONLogLexer(JsonParser()).parse(data2).get("LOG"))
  }

  /*@Test
  def xmllexer(): Unit = {

    val lexer = XMLLogLexer()


    val data =
      """<Root>
        |  <Login>
        |    <AID></AID>
        |    <SID></SID>
        |    <SessionID>E68A368F8A489DB1FB9668C3B6394AC2790368304646383100106455</SessionID>
        |    <GUID>4814307711308982388</GUID>
        |    <ClientNo>E68A368F8A489DB1FB9668C3B6394AC2790368304646383100106455</ClientNo>
        |    <NetBankCookieID></NetBankCookieID>
        |    <LoginIDType>C</LoginIDType>
        |    <LoginID>340322199107168414</LoginID>
        |    <LoginTime>20150827 14:57:28</LoginTime>
        |    <CustomerIP>122.225.65.218</CustomerIP>
        |    <Location></Location>
        |    <CustomerMAC>xhXsFwIrZZ8Dxfyfs0XHZcxZfZU=</CustomerMAC>
        |    <AuthType>A</AuthType>
        |    <DeviceType>H</DeviceType>
        |    <IDType>P01</IDType>
        |    <IDCard>340322199107168414</IDCard>
        |    <Name>?????/Name>
        |    <OperateCode>CMB.Retail.InternetBank.Mobile.General.Login</OperateCode>
        |    <TransCode>7002001</TransCode>
        |    <ResultCode>Y</ResultCode>
        |    <ErrorCode></ErrorCode>
        |    <Remark></Remark>
        |</Login>
        |    </Root>""".stripMargin
    val parser = new WstxSAXParser(new WstxInputFactory, true)

    val elem = XML.withSAXParser(parser).loadString(data)
    println(elem.text)


    /*  XHtml.

     val parser=new SAXParserImpl() with SAXParser {
       override def getProperty(name: String): AnyRef = ???

       override def setProperty(name: String, value: scala.Any): Unit = ???

       override def getParser: sax.Parser = ???
     }()
      val elem=  XML.withSAXParser(parser).loadString(data)
      println(elem.text)*/
    println(lexer.parse(data))
  }*/


  @Test
  def cmb_phone(): Unit = {

    val data = "<Root><Even><AID></AID><SID></SID><OperateCode>T815201</OperateCode><TransCode>T815201</TransCode><SessionID>E68A368F8A489DB1FB9668C3B6394AC2122403022550287300105752</SessionID><ClientNo>E68A368F8A489DB1FB9668C3B6394AC2122403022550287300105752</ClientNo><URL>/USER/UNIONUSERREGISTER/REG_SETPWD.ASPX</URL><AccType>0</AccType><AccNo>13535358282</AccNo><CurrencyType>M</CurrencyType><Amount></Amount><Share></Share><Fee></Fee><OpponentAccNo></OpponentAccNo><CommitTime>20150827 14:57:28</CommitTime><ResultCode>Y</ResultCode><ErrorCode></ErrorCode><ErrorMsg></ErrorMsg><Remark></Remark><CustomerIP>125.95.129.56</CustomerIP><CustomerMAC>000000000842015073051A53462-85A8-4E5D-B91A-7C47C214AEDC0GhY1NR0=</CustomerMAC><GUID>5746587459709709268</GUID><NetBankCookieID></NetBankCookieID><LoginIDType>A</LoginIDType><LoginID>6214860207472347</LoginID><DeviceType>D</DeviceType><IDType>P01</IDType><IDCard>440401196806272118</IDCard><Name>?????/Name><Location></Location></Even></Root>"

    /*  println(data)

      println(data.replaceAll("([^<])(/\\w+>)","$1<$2"))
  */
    val path = "data/cmb.phone/mobilebank-1.log"
    val reader = new TXTFileLogReader(path, None, None, LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))

    val filter = Filter(Array(MapMatch("LOG", Array(
      MapCase("^\\d{8}[\\s\\S]*", ReParser(Some("LOG"),
        Delimit(Some(" "), Array("date", "time", "msg"),
          Array(MapMatch("msg", Array(
            MapCase("^<[\\s\\S]*", ReParser(Some("msg"), XmlParser())),
            MapCase("^\\w\\s+\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}[\\s\\S]*", ReParser(Some("msg"), Delimit(Some("   "), Array("flag", "IP", "msg"))))

          )))
        ))
      )
    ))
    ))
    val parser = JSONLogLexer(JsonParser())
    var total = 0
    var error = 0
    var msg = 0
    reader.foreach(event => {
      total = total + 1
      val row = event.content.replaceAll("([^<])(/\\w+>)", "$1<$2")
      //  val data=parser.parse(row)
      val data = filter.filter(parser.parse(row)).head

      data.get("root") match {
        case Some(d) =>
          println("root:" + data)
          msg = msg + 1
        case _ =>
      }

      if (data.contains("error")) {
        error = error + 1
        println("error:" + data.get("error"))

        //  println(data.get("error"))
      } else {
        data.get("IP") match {
          case Some(d) =>
            println(d)
          case _ =>
          /*println(data.map{
            case (key,value)=>
              (key,value.toString.trim)
          }
         )*/
        }
      }

    })

    println(s"total:$total,error:$error,msg:$msg->rate:${error.toDouble / total.toDouble}")
  }


  @Test
  def cmb_pro(): Unit = {

    val path = "data/cmb.pro/nbwebpb73.User20150623.txt"
    val reader = new TXTFileLogReader(path,
      None, None,
      LocalFileWrapper(Paths.get(path), None), ReadPosition(path, 0, 0))

    val filter = Filter(Array(MapStartWith("msg", Array(MapCase("<", ReParser(Some("msg"), XmlParser())),
      MapCase("4546", ReParser(Some("msg"), Delimit(Some(" "), Array("optCode", "opt", "_info", "info")))),
      MapCase("4431", ReParser(Some("msg"), Delimit(Some(" "), Array("optCode", "user", "opt")))),
      MapCase("4430", ReParser(Some("msg"), Delimit(Some(" "), Array("optCode", "user", "opt", "msg")))),
      MapCase("4205", ReParser(Some("msg"), Delimit(Some(" "), Array("optCode", "user", "guard"),
        Array(
          MapStartWith("guard", Array(
            MapCase("[CMBGUARD]", ReParser(Some("guard"), Delimit(Some(":"), Array("guard", "info"))))
          )),
          ReParser(Some("info"), DelimitWithKeyMap(Some("#"), Some("=")))
        )))
      ))),
      MapCase("4548", ReParser(Some("msg"), Delimit(Some(" "), Array("optCode", "_user", "user", "info")))),
      MapCase("4", ReParser(Some("msg"), Delimit(Some(" "), Array("optCode", "msg"))))
    ))
    val parser = DelimiterLexer(Delimit(Some(" "), Array("date", "time", "host", "msg")))

    reader.foreach(event => {
      val data = filter.filter(parser.parse(event.content)).head
      data.get("CPU") match {
        case Some(_) =>

          data.foreach(println)
          println()
        case _ =>

      }
    })
  }


  @Test
  def zh_zhuanyi_Test(): Unit = {
    /* val line =
       """20150623 00:00:02 114.84.151.34   4546 Login info: <Userid>1000972810</Userid><Cookie></Cookie>
         |20150623 00:00:03 117.80.193.170  4430 2103030996 PBLogin 0021R0K18DI6 customerinfo:3710091257EAP200004624COM302
         |20150623 00:00:03 117.80.193.170  4430 2103030996 PBLogin 99994392250031015638 customerinfo:
         |20150623 00:00:03 117.80.193.170  4430 2103030996 PBLogin successfully.To host at:20150623 00:00:03. [8056F20C0F26],USBKeyType:23
         |20150623 00:00:03                 <Root><Login>  <OperateCode>CMB.Retail.InternetBank.Professional.RemovalCertificate.Login</OperateCode>  <LoginIDType>E</LoginIDType>  <LoginID>2103030996</LoginID>  <SessionID>s1434988802_c21926969_i117.80.193.170_u2103030996_g1</SessionID>  <LoginTime>20150623 00:00:03</LoginTime>  <CustomerIP>117.80.193.170</CustomerIP>  <CustomerMAC></CustomerMAC>  <AuthType>B</AuthType>  <DeviceType>A</DeviceType>  <IDType>P01</IDType>  <IDCard>321323198410173020</IDCard>  <Name>李颖</Name>  <ResultCode>Y</ResultCode>  <ErrorCode></ErrorCode>  <Remark></Remark></Login></Root>
         |20150623 00:00:04 117.80.193.170  4205 2103030996 [CMBGUARD]:SAFEFLAG=0#CALL=LOGIN#00000000CT=DLL#UID=2103030996#LOGINTYPE=LOCAL#Ltime=2015-6-23:0:0:31#LcmpName=HP-201311271128#Luser=Administrator#LIP=:192.168.1.101#LMAC=8056f20c0f26#OIP=ERROR-1#PIP=222.66.171.69#IEPB=00#TRS=ERROR-2#BIOS=5CG3440D1L#BMF=#PMC=8056F20C0F26:A0D3C171BD0D#DS=83889594368#CPU=BFEBFBFF000306A9#ADB=0000#SVRTime=20150622160003#OP=ERROR--5#C=H#FT=1#FTN=1#TT=#TTN=0#VMN=0#CSID=S-1-5-21-960340057-31771193-4044810255#NPI=ERROR--1#Ports=192.168.1.101:61298#163.177.65.143:443#QQGameHall.exe#192.168.1.101:62290#101.199.97.161:80#360tray.exe#192.168.1.101:62311#222.66.171.81:443#PersonalBankPortal.exe#192.168.1.101:62312#23.59.139.27:80#PersonalBankPortal.exe#192.168.1.101:62314#23.59.139.27:80#PersonalBankPortal.exe#192.168.1.101:62315#101.199.97.105:80#360tray.exe#192.168.1.101:62321#222.66.171.69:443#PersonalBankPortal.exe#192.168.1.101:62322#222.66.171.69:443#PersonalBankPortal.exe#""".stripMargin
   */ val line =
      """20150623 00:00:03                 <Root><Login>  <OperateCode>CMB.Retail.InternetBank.Professional.RemovalCertificate.Login</OperateCode>  <LoginIDType>E</LoginIDType>  <LoginID>2103030996</LoginID>  <SessionID>s1434988802_c21926969_i117.80.193.170_u2103030996_g1</SessionID>  <LoginTime>20150623 00:00:03</LoginTime>  <CustomerIP>117.80.193.170</CustomerIP>  <CustomerMAC></CustomerMAC>  <AuthType>B</AuthType>  <DeviceType>A</DeviceType>  <IDType>P01</IDType>  <IDCard>321323198410173020</IDCard>  <Name>李颖</Name>  <ResultCode>Y</ResultCode>  <ErrorCode></ErrorCode>  <Remark></Remark></Login></Root>""".stripMargin

    line.split("\n").foreach(
      data =>
        println(
          Filter(Array(ReParser(Some("msg"), XmlParser()))).filter(
            DelimiterLexer(Delimit(Some(" "), Array("date", "time", "host", "msg")))
              .parse(data)))
    )

  }

  @Test
  def iis_Test(): Unit = {
    val line = "#Fields: date time s-ip cs-method cs-uri-stem s-port c-ip cs-version cs(User-Agent) cs(Referer) sc-status sc-bytes cs-bytes time-taken time-taken time-taken time-taken time-taken time-taken time-taken time-taken"
    println(
      Filter(Array(MapStartWith("date", Array(MapCase("#", AddFields(Map("error" -> "this is iis header,not an valid log data"))))))).filter(
        DelimiterLexer(Delimit(Some(" "), Array("date", "time", "s-sitename", "s-computername", "s-ip", "cs-method", "cs-uri-stem", "cs-uri-query", "s-port", "cs-username", "c-ip", "cs-version", "cs(User-Agent)", "cs(Cookie)", "cs(Referer)", "cs-host", "sc-status", "sc-substatus", "sc-win32-status", "sc-bytes", "cs-bytes", "time-taken")))
          .parse(line)))
  }

  @Test
  def wyxy_lexer(): Unit = {

    val line = ": {\"dt\":\"LEADSEC_IDS_0700R0200B20140925112801\",\"level\":30,\"id\":\"169225718\",\"type\":\"Alert Log\",\"time\":1461726447400,\"source\":{\"ip\":\"192.168.20.5\",\"port\":0,\"mac\":\"0c-c4-7a-7a-f0-1b\"},\"destination\":{\"ip\":\"192.168.20.7\",\"port\":0,\"mac\":\"6c-92-bf-09-b5-b4\"},\"count\":19,\"protocol\":\"FTP\",\"subject\":\"FTP_\",\"message\":\"nic=1;=gap_s_ca;=******;\"}"
    val filter = Filter(Array(ReParser(Some("json"), JsonParser())))
    println(filter.filter(DelimiterLexer(Delimit(Some(":"), Array("_ignore", "json")))
      .parse(line)))
  }

  @Test
  def qiming_lexer(): Unit = {

    val line = "FLOW: SerialNum=0815301408089999 GenTime=\"2016-04-29 11:37:41\" SrcIP=10.0.0.240 DstIP=192.168.5.2 Protocol=TCP SrcPort=41739 DstPort=2040 ProtoNum=6 AppProto=9 Starttime=\"2016-04-29 11:37:37\" Endtime=\"2016-04-29 11:37:41\" Packets=9 Bytes=783 VpnType=0 Vsysid=0 Content=\"\" EvtCount=1"
    val filter = Filter(Array(ReParser(Some("json"), DelimitWithKeyMap(Some(" ")))))
    println(filter.filter(DelimiterLexer(Delimit(Some(":"), Array("_ignore", "json")))
      .parse(line)))
  }


  @Test
  def delimitWithKeyMapTest(): Unit = {
    //TMCM:SLF_INCIDENT_EVT_VIRUS_FOUND_PASS_THRU Security product="OfficeScan" Security product node="SKY-20160323DNS" Security product IP="10.220.5.64" Event time="2016/4/5 18:10:48" Virus="TROJ_GENERIC.APC" Action taken="Move" Result="Unable to quarantine file" Infection destination="SKY-20160323DNS" Infection destination IP="10.220.5.64" Infection source="N/A" Infection source IP="0.0.0.0" Destination IP="0.0.0.0" Source IP="0.0.0.0" Domain="Workgroup" User="\\V-NONAME"
    //id=tos time="2016-04-06 16:53:11" fw=滨海公安外网防火墙 pri=6 type=ac recorder=FW-NAT src=60.30.19.16 dst=123.125.82.246 sport=36951 dport=80 smac=74:25:8a:eb:f6:0e dmac=00:13:32:0c:6a:b7 proto=tcp indev=eth11 outdev=eth10 user= rule=accept connid=531503408 parentid=0 dpiid=0 natid=8059 policyid=8036 msg="null"
    //StartWith("date",Array(Case("#",AddFields(Map("error"->"this is iis header,not an valid log data")))))
    /* println(DelimiterWithKeyMapLexer(DelimitWithKeyMap(Some(" "))).
       parse("id=gw:ips time='2015-05-11 16:34:16' fw=ksgv pri=1 proto=TCP src=10.88.230.42:1628 dst=10.88.137.1:445 rule=2008715 action=drop msg='ET EXPLOIT Microsoft Windows NETAPI Stack Overflow Inbound - MS08-067 (25)' class='Attempted Administrator Privilege Gain'"))
    */ println(DelimiterWithKeyMapLexer(DelimitWithKeyMap(Some(" "))).
      parse(""" zhhuiyan.local id=tos time="2016-04-06 16:53:56" fw=滨海公安外网防火墙 pri=6 type=ac recorder=FW-NAT src=60.29.15.234 dst=60.30.19.1 sport=50661 dport=19190 smac=dc:d2:fc:23:e2:54 dmac=00:13:32:0c:6a:b6 proto=udp indev=eth10 outdev=eth11 user= rule=accept connid=502282544 parentid=0 dpiid=0 natid=0 policyid=8036 msg="null"""".stripMargin))
    println(DelimiterWithKeyMapLexer(DelimitWithKeyMap(Some(" "))).
      parse("""Security product="OfficeScan" Security product node="SKY-20160323DNS" Security product IP="10.220.5.64" Event time="2016/4/5 18:10:48" Virus="TROJ_GENERIC.APC" Action taken="Move" Result="Unable to quarantine file" Infection destination="SKY-20160323DNS" Infection destination IP="10.220.5.64" Infection source="N/A" Infection source IP="0.0.0.0" Destination IP="0.0.0.0" Source IP="0.0.0.0" Domain="Workgroup" User="\\V-NONAME"""".stripMargin))

  }

  @Test
  def ruiXinTest(): Unit = {
    val log = "user-PC [事件日志]事件类型: 扫描病毒\n事件级别: 提示信息\n事件信息: 1：开始时间2015-05-08 14:34:39，结束时间2015-05-08 15:19:38，扫描文件194922个，发现染毒病毒文件24个\n计算机名称: user-PC\nIP: 10.88.132.156\n报告者:RavService\n发现日期: 2015-05-08 15:19:38\n "
    val log1 = "user-PC [病毒日志]病毒名: RootKit.Agent.jy\n病毒类型:Rootkit\n清除结果: 用户忽略\n病毒发现者: 客户端手动查杀\n感染的文件:NTCmd.exe\n感染路径: F:\\hscan120\\hscan120\\tools\n发现日期: 2015-05-08 15:19:28\n计算机名称: user-PC\nIP: 10.88.132.156\n "

    val filter = Filter(Array(MapMatch("type",
      Array(MapCase("事件日志",
        ReParser(Some("log"), DelimitWithKeyMap(Some("\n"), Some(":"))
        )
      ), MapCase("病毒日志",
        ReParser(Some("log"), DelimitWithKeyMap(Some("\n"), Some(":"))
        )
      )
      ))))


    val parser = Regex("%{NOTSPACE:computer} \\[%{NOTSPACE:type}\\]%{ALL:log}")

    println(RegexLexer(parser).parse(log))
    println(filter.filter(RegexLexer(parser).parse(log)))

    //   println(  SelectLogLexer(SelectParser(Array(
    //    Regex("%{NOTSPACE:computer} %{NOTSPACE} %{NOTSPACE:event_type} %{NOTSPACE} %{NOTSPACE:event_level} %{NOTSPACE} %{DATA:event_info}? %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ipaddress} %{NOTSPACE}:\\s*%{NOTSPACE:reportor} %{NOTSPACE} %{DATE TIME:date} %{TIME:time}\\s*"),
    //    Regex("%{NOTSPACE:virus_name} %{NOTSPACE} %{NOTSPACE:virus_type} %{NOTSPACE} %{NOTSPACE:opt_result} %{NOTSPACE} %{NOTSPACE:virus_reportor} %{NOTSPACE}:\\s*%{DATA:infected_file}? %{NOTSPACE}:\\s*%{DATA:infected_path}? %{NOTSPACE} %{DATE:date} %{TIME:time} %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ip}\\s*"))))
    //    .parse("user-PC [病毒日志]病毒名: EICAR-Test-File 病毒类型:感染型病毒 清除结果: 文件被删除 病毒发现者: 实时监控 感染的文件:C:\\USERS\\USER\\DESKTOP\\病毒\\EICAR.COM.TXT 感染路径: C:\\WINDOWS\\EXPLORER.EXE 发现日期: 2015-05-07 16:37:49 计算机名称: user-PC IP: 156.132.88.10"))
  }

  @Test
  def eventCounterTest(): Unit = {
    val log = "1：开始时间2015-05-08 14:34:39，结束时间2015-05-08 15:19:38，扫描文件194922个，发现染毒病毒文件24个"
    println(RegexLexer(Regex("%{INT}%{DATA}%{DATE} %{TIME}%{DATA:data}%{DATE} %{TIME}%{DATA}%{INT:files}%{DATA}%{INT:vs}.*")).parse(log))

  }

  @Test
  def ruiXinEventCounterTest(): Unit = {
    val log = "user-PC [事件日志]事件类型: 扫描病毒\n事件级别: 提示信息\n事件信息: 1：开始时间2015-05-08 14:34:39，结束时间2015-05-08 15:19:38，扫描文件194922个，发现染毒病毒文件24个\n计算机名称: user-PC\nIP: 10.88.132.156\n报告者:RavService\n发现日期: 2015-05-08 15:19:38\n "
    val log1 = "user-PC [病毒日志]病毒名: RootKit.Agent.jy\n病毒类型:Rootkit\n清除结果: 用户忽略\n病毒发现者: 客户端手动查杀\n感染的文件:NTCmd.exe\n感染路径: F:\\hscan120\\hscan120\\tools\n发现日期: 2015-05-08 15:19:28\n计算机名称: user-PC\nIP: 10.88.132.156\n "


    val f = Filter(Array(MapRedirect("event_type", Array(MapCase("扫描病毒", ReParser(Some("event_info"), Regex("%{INT}%{DATA}%{DATE} %{TIME}%{DATA:data}%{DATE} %{TIME}%{DATA}%{INT:files}%{DATA}%{INT:viruses}.*")))))))
    println(f.filter(SelectLogLexer(SelectParser(Array(
      Regex("%{NOTSPACE:computer} %{NOTSPACE}?:\\s*%{NOTSPACE:event_type}\\n%{NOTSPACE}?:\\s*%{NOTSPACE:event_level}\\n%{NOTSPACE}?:\\s*%{DATA:event_info}\\n%{NOTSPACE}?:\\s*%{NOTSPACE:computer_name}\\n%{NOTSPACE}?:\\s*%{IP:ipaddress}\\n%{NOTSPACE}:\\s*%{NOTSPACE:reportor}\\n%{NOTSPACE}?:\\s*%{DATE:date} %{TIME:time}\\n\\s*"
        , Array(MapRedirect("event_type", Array(MapCase("扫描病毒", ReParser(Some("event_info"), Regex("%{INT}%{DATA}%{DATE} %{TIME}%{DATA:data}%{DATE} %{TIME}%{DATA}%{INT:files}%{DATA}%{INT:viruses}.*"))))))),
      Regex("%{NOTSPACE:computer} %{NOTSPACE}?:\\s*%{NOTSPACE:virus_name}\\n%{NOTSPACE}?:\\s*%{NOTSPACE:virus_type}\\n%{NOTSPACE}?:\\s*%{NOTSPACE:opt_result}\\n%{NOTSPACE}?:\\s*%{NOTSPACE:virus_reportor}\\n%{NOTSPACE}:%{DATA:infected_file}?\\n%{NOTSPACE}:\\s*%{DATA:infected_path}?\\n%{NOTSPACE}?:\\s*%{DATE:date} %{TIME:time}\\n%{NOTSPACE}?:\\s*%{NOTSPACE:computer_name}\\n%{NOTSPACE}?:\\s*%{IP:ip}\\n\\s*")
    )))
      .parse(log)))
    //   println(  SelectLogLexer(SelectParser(Array(
    //    Regex("%{NOTSPACE:computer} %{NOTSPACE} %{NOTSPACE:event_type} %{NOTSPACE} %{NOTSPACE:event_level} %{NOTSPACE} %{DATA:event_info}? %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ipaddress} %{NOTSPACE}:\\s*%{NOTSPACE:reportor} %{NOTSPACE} %{DATE TIME:date} %{TIME:time}\\s*"),
    //    Regex("%{NOTSPACE:virus_name} %{NOTSPACE} %{NOTSPACE:virus_type} %{NOTSPACE} %{NOTSPACE:opt_result} %{NOTSPACE} %{NOTSPACE:virus_reportor} %{NOTSPACE}:\\s*%{DATA:infected_file}? %{NOTSPACE}:\\s*%{DATA:infected_path}? %{NOTSPACE} %{DATE:date} %{TIME:time} %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ip}\\s*"))))
    //    .parse("user-PC [病毒日志]病毒名: EICAR-Test-File 病毒类型:感染型病毒 清除结果: 文件被删除 病毒发现者: 实时监控 感染的文件:C:\\USERS\\USER\\DESKTOP\\病毒\\EICAR.COM.TXT 感染路径: C:\\WINDOWS\\EXPLORER.EXE 发现日期: 2015-05-07 16:37:49 计算机名称: user-PC IP: 156.132.88.10"))
  }


  @Test
  def selectLexerTest(): Unit = {
    val lexer = SelectLogLexer(SelectParser(Array(Regex( """%{NOTSPACE:computer} %{NOTSPACE} %{NOTSPACE:event_type} %{NOTSPACE} %{NOTSPACE:event_level} %{NOTSPACE} %{DATA:event_info}? %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ipaddress} %{NOTSPACE}:\s*%{NOTSPACE:reportor} %{NOTSPACE} %{DATE:date} %{TIME:time}\s*"""),
      Regex( """%{NOTSPACE:virus_name} %{NOTSPACE} %{NOTSPACE:virus_type} %{NOTSPACE} %{NOTSPACE:opt_result} %{NOTSPACE} %{NOTSPACE:virus_reportor} %{NOTSPACE}:\s*%{DATA:infected_file}? %{NOTSPACE}:\s*%{DATA:infected_path}? %{NOTSPACE} %{DATE:date} %{TIME:time} %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ip}\s*"""))))
    println(lexer.parse( """user-PC [病毒日志]病毒名:Worm.Win32.Gamarue.n 病毒类型:蠕虫 清除结果: 已清除 病毒发现者: 实时监控 感染的文件:C:\USERS\USER\DESKTOP\U盘病毒样本\新建文件夹 (2)\~$WQIXOGHK.FAT 感染路径: C:\PROGRAM FILES\WINRAR\WINRAR.EXE 发现日期: 2015-05-07 16:10:57 计算机名称: user-PC IP: 156.132.88.10 """).toString())
    println(lexer.parse( """USER-PC [事件日志]事件类型: 扫描病毒 事件级别: 提示信息 事件信息: 1：开始时间2015-05-07 14:48:36，结束时间2015-05-07 14:50:38，扫描文件2625个，发现染毒病毒文件0个，清除病毒0个 计算机名称: USER-PC IP: 10.88.132.156 报告者:RavService 发现日期: 2015-05-07 14:50:38 """).toString())
    println(lexer.parse( """USER-20141020TL [事件日志]事件类型: 升级 事件级别: 提示信息 事件信息: 升级瑞星杀毒软件(22.03.50.00)成功 计算机名称: USER-20141020TL IP: 10.88.133.36 报告者:SetupRav.exe 发现日期: 2015-05-08 11:16:51 """).toString())
  }

  @Test
  def esreplaceTest(): Unit = {
    println("AUzSsEdM_8yPQCXDc587{\"asset_id\":\"asset_id_1\",\"fw\":\"滨海公安防火墙-主\",\"outpkt\":\"0\",\"dport\":\"137\",\"parentid\":\"parentid\",\"dst\":\"10.88.134.255\",\"type\":\"conn\",\"arg\":\"arg\",\"time\":\"2015-04-20 01:44:18\",\"sent\":\"78\",\"dpiid\":\"dpiid\",\"level\":\"-1\",\"rcvd\":\"0\",\"dmac\":\"dmac\",\"indev\":\"indev\",\"host\":\"10.88.133.252\",\"op\":\"delete\",\"asset_department_tree_name\":\"督察部\",\"parent\":\"0\",\"msg\":null,\"@timestamp\":\"2015-04-19T17:44:18.000Z\",\"recorder\":\"session\",\"asset_tree_name\":\"天融信防火墙主\",\"raw\":\"id=tos time=\\\"2015-04-20 01:44:18\\\" fw=滨海公安防火墙-主  pri=6 type=conn  recorder=session src=10.88.134.111 dst=10.88.134.255 proto=udp sport=137 dport=137 inpkt=1 outpkt=0 sent=78 rcvd=0 duration=0 connid=546572550 parent=0 op=delete msg=\\\"null\\\"\\r\\n\",\"result\":\"result\",\"collector.host\":\"10.88.136.25\",\"collector.app\":\"appDelimit\",\"asset_tree_id\":\"asset_tree_id_9\",\"timestamp\":\"2015-04-19T17:17:58.513Z\",\"id\":\"tos\",\"facility\":\"-1\",\"rule\":\"rule\",\"path\":\"udp://0.0.0.0:5144\",\"src\":\"10.88.134.111\",\"asset_department_tree_id\":\"department_tree_id_1\",\"collector.port\":5144,\"asset_name\":\"天融信防火墙-主\",\"sport\":\"137\",\"proto\":\"udp\",\"asset_department_tree_code\":\"department_tree_id_0-department_tree_id_1-\",\"outdev\":\"outdev\",\"duration\":\"0\",\"pri\":\"6\",\"is_event\":false,\"connid\":\"546572550\",\"smac\":\"smac\",\"inpkt\":\"1\",\"user\":\"user\",\"policyid\":\"policyid\"}"
      .replaceFirst("\"host\"(\\:\"\\d+\\.\\d+\\.\\d+\\.\\d+\")", "\"host\"$1,\"person\"$1"))


  }

  @Test
  def quoteString(): Unit = {

    "\"7\"" match {
      case QuoteString(a, b, c) =>
        println(b)
        assert(!b.contains("\'"), "must be match ")
    }

    "\"abc=1233 sdasda\"" match {
      case QuoteString(a, b, c) =>
        println(b)
        assert(!b.contains("\'"), "must be match ")
    }
    "\'abc" match {
      case QuoteString(a, b, c) =>
        println(b)
        assert(!b.contains("\'"), "must be match ")
    }
    "\'abc\"" match {
      case QuoteString(a, b, c) =>
        println(b)
        assert(!b.contains("\'"), "must be match ")
    }
    "\'abc\'" match {
      case QuoteString(a, b, c) =>
        println(b)
        assert(!b.contains("\'"), "must be match ")
    }
    "\'ab\"c\'" match {
      case QuoteString(a, b, c) =>
        println(b)
        assert(b.contains("\""), "must be match ")

    }
    "\'ab\"123\" c\'" match {
      case QuoteString(a, b, c) =>
        println(b)
        assert(b.contains("\"123\""), "must be match ")
    }

    """"服务端口: 80
      |源端口: 64490
      |协议: TCP
      |附件信息:
      |终端类型: PC(Windows PC)
      |文件类型: 其他
      |文件名: 250-250.flv
      |网站: dian.tianfus.com
      |URL: dian.tianfus.com/ts2052/ts/250-250.flv
      |DNS: api.plu.cn.safe.cdntip.com"""".stripMargin match {
      case QuoteString(a, b, c) =>


        println(a, b, c)
        assert(!b.contains("\""), "must be match ")
      case v =>

        println(v)
        assert(false, "must be not run ")
    }
  }

  @Test
  def stringLenTest(): Unit = {
    assert("123".matches("\\d+"))

    val bytes = new Array[Byte](1000)
    assert(!(new String(bytes, 0, bytes.length) + ":::").equals(":::"))
  }

  @Test
  def lostYearTest(): Unit = {
    val log = """Jan 15 15:19:56 centos sshd[1038]: Server listening on 0.0.0.0 port 22."""
    val filter = Filter(Array(AddFields(Map("@timestamp" -> "%{datetime} 2015"))))
    val parser = Regex("%{DATESTAMP:@timestamp} %{GREEDYDATA:all}", filter.rules)
    println(filter.filter(RegexLexer(parser).parse(log)))
  }
}
