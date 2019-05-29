package com.haima.sage.bigdata.etl.server

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import akka.actor._
import com.haima.sage.bigdata.etl.codec.{DelimitCodec, Contain => CodecContain}
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration

/**
  * Created by zhhuiyan on 2015/2/4.
  */
object ConfigSender extends App {
  //  val logger: Logger = LoggerFactory.getLogger(classOf[ConfigSender])
  val system = ActorSystem("test", ConfigFactory.load("serverTest.conf"))
  val path = "akka.tcp://worker@10.20.5.189:19093/user/server"
  val sender = system.actorOf(Props(classOf[ConfigSender], path), name = "sender")
  TimeUnit.SECONDS.sleep(1)
  // sender ! "ruixing"
  // sender ! "ruixing-stop"
  //sender ! "lnyd-create"
  //sender ! "std-update"
  //sender ! "std-delete"
  // sender ! "lnyd-start"
  // sender ! "lnyd-stop"

  // sender ! "std-create"
  //sender ! "std-update"
  //sender ! "std-delete"
  // sender ! "std-start"
  // sender ! "std-stop"
  //    sender ! "file-create"
  // sender ! "pipe-create"

  /*TimeUnit.SECONDS.sleep(1)
  // sender ! "pipe-start"
  TimeUnit.SECONDS.sleep(1)
  sender ! "file-start"
  */
  //   TimeUnit.SECONDS.sleep(1)
  //  sender ! "file-start"

  /* sender ! "es-create"

 TimeUnit.SECONDS.sleep(5)
 sender ! "es-start"*/
  // TimeUnit.SECONDS.sleep(1)
  //sender ! "file-stop"
  //  sender ! "file-update"
  //sender ! "file-delete"

  /*sender ! "ftp-create"
  TimeUnit.SECONDS.sleep(5)
  sender ! "ftp-start"*/

  /*  sender ! "hdfs-create"
 TimeUnit.SECONDS.sleep(5)
 sender ! "hdfs-start"*/

  // sender ! "h3c-update"
  //sender ! "h3c-delete"
  // sender ! "h3c-start"
  // sender ! "h3c-stop"

  /* sender ! "h3c-create"
 TimeUnit.SECONDS.sleep(1)
 sender ! "h3c-start"*/
  /*(0 to 100 ).foreach{
 i=>
 sender ! "h3c-start"
 logger.debug(s"asdasdasd:$i")
 TimeUnit.SECONDS.sleep(1)
 sender ! "h3c-stop"
 TimeUnit.SECONDS.sleep(1)
  }*/

  // sender ! "h3c-regex"
  /* sender ! "syslog-create"
 sender ! "syslog-start"
 TimeUnit.SECONDS.sleep(10)
 sender ! "syslog-stop"*/

  /*sender ! "syslog-stop"
  sender ! "syslog-create"*/
  /*TimeUnit.SECONDS.sleep(3)
  sender ! "syslog-start"*/
  /*sender ! "syslog-stop"
 sender ! "syslog-create"
  TimeUnit.SECONDS.sleep(1)
 /* sender ! "syslog-update"
 sender ! "syslog-delete"*/
 sender ! "syslog-start"
  TimeUnit.SECONDS.sleep(3)
 sender ! "syslog-stop"
*/
  /*  sender ! "jdbc-stop"
    TimeUnit.SECONDS.sleep(3)
    sender ! "jdbc-create"
    TimeUnit.SECONDS.sleep(3)
    sender ! "jdbc-start"*/
  //  sender ! "h3c-regex"
  /* sender ! "jdbc-update"
   TimeUnit.SECONDS.sleep(3)
   sender ! "jdbc-start"
   TimeUnit.SECONDS.sleep(3)
   //  sender ! "jdbc-stop"
   TimeUnit.SECONDS.sleep(10)*/
  //sender ! "trx"
  // sender ! "status"
  sender ! "flink-sql"
}


class ConfigSender(path: String) extends Actor {

  val configs = List()


  private[server] val logger: Logger = LoggerFactory.getLogger(classOf[ConfigSender])
  sendIdentifyRequest()

  def sendIdentifyRequest(): Unit = {
    context.actorSelection(path) ! Identify(path)
  }

  def receive = identifying

  def identifying: Receive = {
    case ActorIdentity(`path`, Some(actor)) =>
      context.watch(actor)
      context.become(active(actor))
      context.setReceiveTimeout(Duration.Undefined)
      logger.info("serverClient is start!")
    case ActorIdentity(`path`, None) =>
      logger.warn(s"Remote actor not available: $path")
      sendIdentifyRequest()
    case ReceiveTimeout =>
      logger.info("serverClient is timeout !")
      sendIdentifyRequest()
  }

  def active(actor: ActorRef): Receive = {
    case "status" =>
      actor ! ("0", "HEALTH")
    case "lnyd-create" =>
      configs.foreach {
        conf => actor ! conf
      }
    case "lnyd-start" =>
      configs.foreach {
        conf => actor ! (conf, START)
      }
    case "lnyd-stop" =>
      configs.foreach {
        conf => actor ! (conf, STOP)
      }
    case "ruixing-stop" =>
      val config =
        Config("std_0","testSingle",
          SingleChannel(NetSource(Some(Syslog()), port = 5157),
          Some(SelectParser(Array(
            Regex("%{NOTSPACE:computer} %{NOTSPACE} %{NOTSPACE:event_type} %{NOTSPACE} %{NOTSPACE:event_level} %{NOTSPACE} %{DATA:event_info}? %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ipaddress} %{NOTSPACE}:\\s*%{NOTSPACE:reportor} %{NOTSPACE} %{DATE:date} %{TIME:time}\\s*"),
            Regex("%{NOTSPACE:virus_name} %{NOTSPACE} %{NOTSPACE:virus_type} %{NOTSPACE} %{NOTSPACE:opt_result} %{NOTSPACE} %{NOTSPACE:virus_reportor} %{NOTSPACE}:\\s*%{DATA:infected_file}? %{NOTSPACE}:\\s*%{DATA:infected_path}? %{NOTSPACE} %{DATE:date} %{TIME:time} %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ip}\\s*"))))),
          Some(Collector("std_collector_0", "localhost", 5151)),
          List(Writer("std_writer_0", "std", 1)))
      actor ! (config, STOP)
    case "ruixing" =>

      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      val config =
        Config("std_0","testSingle",
          SingleChannel((NetSource(Some(Syslog()), port = 5157)),
          Some(SelectParser(Array(
            Regex("%{NOTSPACE:computer} %{NOTSPACE} %{NOTSPACE:event_type} %{NOTSPACE} %{NOTSPACE:event_level} %{NOTSPACE} %{DATA:event_info}? %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ipaddress} %{NOTSPACE}:\\s*%{NOTSPACE:reportor} %{NOTSPACE} %{DATE:date} %{TIME:time}\\s*"),
            Regex("%{NOTSPACE:virus_name} %{NOTSPACE} %{NOTSPACE:virus_type} %{NOTSPACE} %{NOTSPACE:opt_result} %{NOTSPACE} %{NOTSPACE:virus_reportor} %{NOTSPACE}:\\s*%{DATA:infected_file}? %{NOTSPACE}:\\s*%{DATA:infected_path}? %{NOTSPACE} %{DATE:date} %{TIME:time} %{NOTSPACE} %{NOTSPACE:computer_name} %{NOTSPACE} %{IP:ip}\\s*"))))),
          Some(Collector("std_collector_0", "localhost", 5151)),
          List(Writer("std_writer_0", "std", 1)))
      actor ! config
      actor ! (config, START)
    case "std-create" =>

      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),

      val conf = Config("std_0","testSingle",
        SingleChannel(DataSource("std", "std"),
          Some(NothingParser(Array(AddFields(Map("test" -> "124")))))),
        Some(Collector("std_collector_0", "localhost", 5151)),
        List(Writer("std_writer_0", "std", 1)))
      actor ! (conf, UPDATE)

    case "std-update" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),

      actor ! (Config("std_0","testSingle",
        SingleChannel((DataSource("std", "std")),
          Some(NothingParser(Array(AddFields(Map("test" -> "124")))))),
        Some(Collector("std_collector_0", "localhost", 5151)),
        List(Writer("std_writer_0", "std", 1), ES2Writer("std_writer_1", "elasticsearch", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1))), UPDATE);


    case "std-stop" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("std_0","testSingle"), STOP)

    case "std-start" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("std_0","testSingle"), START)
    case "jdbc-update" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      //(q_exam_problem_leak leak LEFT JOIN q_exam exam on leak.exam_id=exam.exam_id)
      val postgresql = (Config("jdbc_0","testSingle",
        SingleChannel((JDBCSource("postgresql", "org.postgresql.Driver", host = "172.16.219.95", port = 5432,
          properties = Properties(Map("user" -> "ent", "password" -> "ent")).properties, schema = "Report",
          tableOrSql =
            """(SELECT leak.last_date AS leak_last_date, *,'leak' AS type  FROM (q_exam_problem_leak leak LEFT JOIN q_exam exam ON leak.exam_id = exam.exam_id LEFT JOIN q_user users ON leak.s_mid = users.s_mid) UNION SELECT leak.last_date AS leak_last_date, *, 'perilsys' AS type FROM (q_exam_problem_perilsys leak LEFT JOIN q_exam exam ON leak.exam_id = exam.exam_id LEFT JOIN q_user users ON leak.s_mid = users.s_mid) UNION SELECT leak.last_date AS leak_last_date, *, 'plugin' AS type FROM (q_exam_problem_plugin leak LEFT JOIN q_exam exam ON leak.exam_id = exam.exam_id LEFT JOIN q_user users ON leak.s_mid = users.s_mid) UNION SELECT leak.last_date AS leak_last_date, *, 'safe' AS type FROM (q_exam_problem_safe leak LEFT JOIN q_exam exam ON leak.exam_id = exam.exam_id LEFT JOIN q_user users ON leak.s_mid = users.s_mid) UNION SELECT leak.last_date AS leak_last_date, *, 'trojan' AS type FROM (q_exam_problem_trojan leak LEFT JOIN q_exam exam ON leak.exam_id = exam.exam_id LEFT JOIN q_user users ON leak.s_mid = users.s_mid) UNION SELECT leak.last_date AS leak_last_date, *, 'other' AS type FROM (q_exam_problem_other leak LEFT JOIN q_exam exam ON leak.exam_id = exam.exam_id LEFT JOIN q_user users ON leak.s_mid = users.s_mid)) as exams """.stripMargin,
          column = "leak_last_date", start = "2015-06-26 16:00:00.000", step = 60 * 60 * 24)),
          Some(CefParser(Array(Mapping(Map("last_date" -> "@timestamp")))))),
        Some(Collector("jdbc_0", "localhost", 5151)),
        List(SyslogWriter(port = 2015), ES2Writer("1", "elasticsearch", Array(("172.16.219.130",9200)), s"logs_%{yyyyMMdd}", "leak", cache = 1))), UPDATE)


      val orcle = Config("jdbc_0","testSingle",
        SingleChannel((JDBCSource("oracle", "oracle.jdbc.driver.OracleDriver", host = "172.16.219.20", port = 1521, properties = Properties(Map("user" -> "sdsdasd", "password" -> "123456")).properties, schema = "orcl", tableOrSql = "T_TEST", column = "datetime", start = "2015-10-10 10:10:10", step = 1)),
          Some(CefParser(Array(AddFields(Map("test" -> "124")))))),
        Some(Collector("jdbc_0", "localhost", 5151)),
        List(Writer("jdbc_0", "std", 1000), ES2Writer("1", "elasticsearch", Array(("172.16.219.130",9200)), "logs_spark_stream", "H3C", cache = 1)))

      actor ! (postgresql, UPDATE)
    case "_trx" =>
      val config = Config("trx_4303","testSingle",
        SingleChannel(NetSource(Some(Syslog()), port = 4303),
          Some(DelimitWithKeyMap(Some(" ")))),
        Some(Collector("h3c0", "localhost", 5151)),
        List(StdWriter("std_writer_0")))
      actor ! (config, CREATE)

      TimeUnit.SECONDS.sleep(10l)
      actor ! (config, START)
    case "trx" =>
      val config = Config("trx_4303","testSingle",
        SingleChannel((NetSource(Some(Syslog()), port = 4303)),
          Some(TransferParser(filter = Array(Drop())))),
        Some(Collector("h3c0", "localhost", 5151)),
        List(StdWriter("std_writer_0")))
      actor ! (config, CREATE)

      TimeUnit.SECONDS.sleep(10l)
      actor ! (config, START)

    //%%01AGENTLOG/6/LOGONOROFF(l): The agent logged on.  user="Dongxing.Sun@faw-vw.in" hostname="00001430L2" ip="10.238.64.233" logtype="1" authtype="4" time="2016-04-11 13:13:53" mac="D4-BE-D9-2D-A8-65" result="1"
    case "jdbc-create" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      //(q_exam_problem_leak leak LEFT JOIN q_exam exam on leak.exam_id=exam.exam_id)
      val postgresql = Config("jdbc_0","testSingle",
        SingleChannel((JDBCSource("postgresql", "org.postgresql.Driver", host = "172.16.219.79", port = 5432,
          properties = Properties(Map("user" -> "postgres", "password" -> "postgres")).properties, schema = "hla_webcenter",
          tableOrSql = "select dt.id,dt.name,concat(date,' ',time) datetime from dv_test dt",
          column = "datetime", start = "2016-05-04 16:00:00.000", step = 60 * 60 * 24)),
          Some(CefParser(Array(Mapping(Map("last_date" -> "@timestamp")))))),
        Some(Collector("jdbc_0", "localhost", 5151)),
        List(Writer("1", "std", 1)))


      val orcle = Config("jdbc_0","testSingle",
        SingleChannel((JDBCSource("oracle", "oracle.jdbc.driver.OracleDriver", host = "172.16.219.20", port = 1521, properties = Properties(Map("user" -> "sdsdsd", "password" -> "123456")).properties, schema = "orcl", tableOrSql = "T_TEST", column = "datetime", start = "2015-10-10 10:10:10", step = 1)),
          Some(CefParser(Array(AddFields(Map("test" -> "124")))))),
        Some(Collector("jdbc_0", "localhost", 5151)),
        List(Writer("jdbc_0", "std", 1000), ES2Writer("1", "elasticsearch", Array(("172.16.219.130",9200)), "logs_spark_stream", "H3C", cache = 1)))

      actor ! (postgresql, CREATE)
    case "jdbc-stop" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("jdbc_0","testSingle"), STOP)

    case "jdbc-start" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("jdbc_0","testSingle"), START)
    case "h3c-regex" =>


      val conf = Config(s"h3c_regex","testSingle",
        SingleChannel((NetSource(Some(Syslog()), port = 4303)),
          Some(DelimitWithKeyMap(Some(";"), Some(":")))),
        Some(Collector("h3c0", "localhost", 5151)),
        List(ForwardWriter(host = "127.0.0.1", port = 19093)))
      // println(conf.toJson.prettyPrint)
      /*TcpWriter(host = "127.0.0.1",port = 1212,cache =1))*/

      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (conf, CREATE)
      TimeUnit.SECONDS.sleep(1)
      actor ! (conf, START)
      TimeUnit.SECONDS.sleep(2)
      /*"spark://yzh:7077"*/
//      val config = Config("spark_stream",
//        Some(NetSource("pipe", Some("akka"), Some("127.0.0.1"), 19093)),
//        Some(SQLAnalyzer("spark://yzh:7077", Array(("dest", "string"), ("dport", "string")),
//          "test",
//          "select dest,count(dest) as count from test group by dest",
//          Array("dest", "count"))),
//        Some(Collector("d4577ce9-94d3-4a0c-96dd-6037aa379181", "local", 19093)),
//        List(ES2Writer("1", "elasticsearch", Array(("127.0.0.1",9200)), "logs_spark_stream", "H3C", cache = 1)))
//      /*
//      *  val config = Config("spark_stream",
//        Some(SparkStreamSource("local[4]", "127.0.0.1", 19093, "worker", "publisher",WordCountHandler(delimit = Some("[\\.\\s\\:;]")))),
//        None,
//        Some(Collector("d4577ce9-94d3-4a0c-96dd-6037aa379181", "local", 19093)),
//        List(ES2Writer("1", "elasticsearch", Array(("127.0.0.1",9200)), "logs_spark_stream", "H3C", cache = 1)))*/
//      TimeUnit.SECONDS.sleep(1)
//      actor ! (config, CREATE)
//      TimeUnit.SECONDS.sleep(1)
//      actor ! (config, START)
    case "flink-sql" =>
      val conf = Config("flink-sql","testSingle",
        SingleChannel((KafkaSource("10.10.106.153:9092", Some("beat"))),
          Some(JsonParser())),
        Some(Collector("flink-sql", "localhost", 19093)),
        List(ForwardWriter(host = "10.20.5.189", port = 19093)))
      // println(conf.toJson.prettyPrint)
      /*TcpWriter(host = "127.0.0.1",port = 1212,cache =1))


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),*/
//      actor ! (conf, STOP)
      actor ! (conf, DELETE)
      actor ! (conf, CREATE)
      TimeUnit.SECONDS.sleep(2)
      actor ! (conf, START)

      TimeUnit.SECONDS.sleep(4)
//      val config = Config("flink-stream",
//        Some(NetSource("pipe", Some("akka"), Some("127.0.0.1"), 19093)),
//        Some(SQLAnalyzer("bi03:6123", Array(("type", "string"),
//          ("@timestamp", "string")), "test", "select * from test", Array("type", "timestamp"))), writers = List(StdWriter()))
//      actor ! (config, DELETE)
//      actor ! (config, CREATE)
//      TimeUnit.SECONDS.sleep(2)
//      actor ! (config, START)
      TimeUnit.SECONDS.sleep(2)


    case "h3c-create" =>
      val conf = Config(s"h3c_0","testSingle",
        SingleChannel((NetSource(Some(Syslog()), port = 4303, codec = Some(DelimitCodec("\r\n")))),
          Some(DelimitWithKeyMap(Some(" ")))),
        Some(Collector("h3c0", "localhost", 5151)),
        List(ES2Writer("1", "elasticsearch", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "H3C", cache = 1000)))
      /*TcpWriter(host = "127.0.0.1",port = 1212,cache =1))*/

      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (conf, CREATE)

    case "h3c-update" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      val conf = Config("h3c_4303","testSingle",
        SingleChannel((NetSource(Some(Syslog()), port = 4303, codec = Some(DelimitCodec("\r\n")))),
          Some(DelimitWithKeyMap(Some(" ")))),
        Some(Collector("h3c0", "localhost", 5151)),
        List(ES2Writer("1", "elasticsearch", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "H3C", cache = 1)))
      actor ! (conf, CREATE)

    case "h3c-delete" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("h3c0","testSingle"), DELETE)
    case "h3c-stop" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config(s"h3c_0","testSingle"), STOP)

    case "h3c-start" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config(s"h3c_0","testSingle"), START)

    case "syslog-create" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),

      val conf = Config("syslog0","testSingle",
        SingleChannel((NetSource(Some(Syslog()), port = 4301, properties = Properties().properties
        )),
          Some(Regex("%{H3C_LOG}"))),
        Some(Collector("syslog0", "localhost", 5151)),
        List(ES2Writer("1", "elasticsearch", Array(("172.16.219.130",9200)), "logs_%{yyyyMMdd}", "H3C", cache = 1)))
      actor ! (conf, CREATE)
    case "syslog-update" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),

      actor ! (Config("syslog0","testSingle",
        SingleChannel((NetSource(Some(Syslog()), port = 4301,
          properties = Properties().properties)),
          Some(Regex("%{H3C_LOG}", Array(AddFields(Map("test" -> "124")))))),
        Some(Collector("syslog0", "localhost", 5151)),
        List(ES2Writer("1", "elasticsearch", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "H3C", cache = 1))), UPDATE)

    case "syslog-delete" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("syslog0","testSingle"), DELETE)
    case "syslog-stop" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("syslog0","testSingle"), STOP)

    case "syslog-start" =>


      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("syslog0","testSingle"), START)

    case "file-create" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      /*  val filter = Filter( Array(AddFields(Map("@timestamp"->"%{AGENT_TIMESTAMP}")),Match("LOG", Array(
      Case("^\\d{8}[\\s\\S]*", Analyzer(Some("LOG"),
      Delimit(Some(" "), Array("date", "time", "msg"),
      Some(Filter(Array(Match("msg", Array(
      Case("^<[\\s\\S]*",Analyzer(Some("msg"), XmlParser())),
      Case("^\\w\\s+\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}[\\s\\S]*",Analyzer(Some("msg"), Delimit(Some(" "),Array("flag","IP","msg"))))

      )))
      ))
      )
      ))
      ))))
      val parser = JsonParser(, Some(filter))*/


      /*val parser = DelimitWithKeyMap(Some(";"),Some("="))//Regex("%{SYSLOGTIMESTAMP:@timestamp} %{NOTSPACE:from} %{DATA:type}: %{ALL:logs}", None)

      val config=Config("file0",
        Some(FileSource("data/internet.log", "commerce", None, None, None, Some(ProcessFrom.START), 1,
          None)),
        Some(/*Delimit(fields = Array("SNO","name","CNO","gender"))*/ parser),
        Some(Collector("0", "localhost", 5151)), List(ES2Writer("1", "elasticsearch", Array(("172.16.219.130",9200)), "logs_internet", "internet", 1)))
*/
      val config = Config("file0","testSingle",
        SingleChannel((FileSource("data/iis/", Option("other"))),
          Some(Regex("%{IIS_LOG}", Array(AddFields(Map(("test", "124"))))))),
        Some(Collector("0", "localhost", 5151)), List(ES2Writer("1", "elasticsearch", Array(("172.16.219.130",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1)))


      actor ! (config, CREATE)
    /*val sysyemOut = Config("file0",
    Some(FileSource("data/av_log/SystemOut.log", "SystemOut",
    codec = Some(MultiCodec("[\\[]", Some(true), None)))),
    Some(Regex("[\\[]%{GREEDYDATA:@timestamp}[\\]][\\s]+%{NOTSPACE:id}[\\s]+%{NOTSPACE:appname}[\\s]+%{NOTSPACE:cmd}[\\s]+%{NOTSPACE:time}[\\s]+%{NOTSPACE:level}[\\s]+[\\[]%{GREEDYDATA:subsys}[\\]][\\s]+%{ALL:msg}",
    Some(Filter(Array(AddFields(Map("source_type" -> "system_out")), Contain("msg", Array(
    Case("====:", Analyzer(Some("msg"),
    Delimit(Some("====:"), Array("msg", "xml"), Some(Filter(Array(Analyzer(Some("xml"), XmlParser())))))
    )))),
    StartWith("msg", Array(Case("<?xml", Analyzer(Some("msg"), XmlParser()))))
    ))))),
    Some(Collector("worker219", "172.16.219.219", 19093)), List(ES2Writer("gkh", "gkh", Array(("172.16.219.219",9200)))))
    val sysyemErr = Config("file0",
    Some(FileSource("data/av_log/SystemErr.log", "SystemErr",
    codec = Some(MultiCodec("\\[.*?\\]\\s\\w+\\s\\w+\\s+\\w\\s\\w+",Some(true))))),
    Some(Regex("\\[%{DATESTAMP:@timestamp}\\]\\s+%{NOTSPACE:id}\\s+%{NOTSPACE:appname}\\s+%{NOTSPACE:cmd}\\s+%{ALL:msg}",
    Some(Filter(Array(AddFields(Map("source_type" -> "system_err")),Match("msg", Array(Case("^\\d+[\\s\\S]*", Analyzer(Some("msg"), Delimit(Some(" "), Array("_date", "_time", "msg")))))), Analyzer(Some("msg"), Delimit(Some("\t"), Array("msg", "Info")))))))
    ),
    Some(Collector("worker219", "172.16.219.219", 19093)), List(ES2Writer("gkh", "gkh", Array(("172.16.219.219",9200)))))

    val alert_avcon = Config("file0",
    Some(FileSource("data/av_log/alert_avcon.log", "alert_avcon",encoding = Some("UTF-8"),
    codec = Some(MultiCodec("\\w{3}\\s+\\w{3}\\s+\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}\\s+\\d{4}", Some(true), None)))),
    Some(Regex(s"%{DATESTAMP:@timestamp}\t%{ALL:LOG}",
    Some(Filter(Array(AddFields(Map("source_type" -> "alert_avcon")), AddFields(Map("bak" -> "%{LOG}")), Analyzer(Some("bak"), Delimit(Some(" "), Array("opt_1","opt_2","opt_3", "_ignore"))),Merger(Array("opt_1","opt_2","opt_3"),"opt"), StartWith("LOG", Array(
    Case("Errors", Analyzer(Some("LOG"), Delimit(Some("\t"), Array("msg", "info"), Some(Filter(Array(StartWith("info", Array(Case("ORA", Analyzer(Some("info"), DelimitWithKeyMap(Some("\t"), Some(": ")))))))))))),
    Case("Thread", Analyzer(Some("LOG"), Delimit(Some("\t"), Array("msg", "info"))))))))))),
    Some(Collector("worker219", "172.16.219.219", 19093)), List(ES2Writer("gkh",  "gkh", Array(("172.16.219.219",9200)))))

   */
    /*
    val sysyemOut =Config("file0",
    Some(FileSource( "data/av_log/SystemOut.log", "sysyemOutDs",
    codec = Some(MultiCodec("[\\[]",Some(true),None)))),
    Some(Regex("[\\[]%{GREEDYDATA:@timestamp}[\\]][\\s]+%{NOTSPACE:id}[\\s]+%{NOTSPACE:appname}[\\s]+%{NOTSPACE:cmd}[\\s]+%{NOTSPACE:time}[\\s]+%{NOTSPACE:level}[\\s]+[\\[]%{GREEDYDATA:subsys}[\\]][\\s]+%{ALL:msg}",Some(Filter(Array(Contain("msg", Array(
    Case("====:", Analyzer(Some("msg"),
    Delimit(Some("====:"), Array("msg", "xml"), Some(Filter(Array(Analyzer(Some("xml"), XmlParser())))))
    )))),
    StartWith("msg", Array(Case("<?xml", Analyzer(Some("msg"), XmlParser()))))
    ))))),
    Some(Collector("0", "localhost", 5151)), List(ES2Writer("e1", "elasticsearch", Array(("172.16.219.130",9200)))))

    val alert_avcon =Config("file0",
    Some(FileSource( "data/av_log/alert_avcon.log", "alert_avcon",
    codec = Some(MultiCodec("\\w{3}\\s+\\w{3}\\s+\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}\\s+\\d{4}",Some(true),None)))),
    Some(Regex(s"%{DATESTAMP:@timestamp}\t%{ALL:LOG}",
    Some(Filter(Array(AddFields(Map("bak"->"%{LOG}")),Analyzer(Some("bak"),Delimit(Some(" "),Array("opt","_ignore"))),StartWith("LOG",Array(
    Case("Errors",Analyzer(Some("LOG"),Delimit(Some("\t"),Array("message","info"),Some(Filter(Array(StartWith("info",Array(Case("ORA", Analyzer(Some("info"), DelimitWithKeyMap(Some("\t"),Some(": ")))))))))))),
    Case("Thread",Analyzer(Some("LOG"),Delimit(Some("\t"),Array("message","info"))))))))))),
    Some(Collector("0", "localhost", 5151)), List(ES2Writer("e1", "elasticsearch", Array(("172.16.219.130",9200)))))

    val sysyemErr =Config("file0",
    Some(FileSource( "data/av_log/SystemErr.log", "sysyemOutDs",
    codec = Some(MatchCodec("[\\[]",Some(CodecContain("at")))))),
    Some(Regex("[\\[]%{GREEDYDATA:@timestamp}[\\]][\\s]+%{NOTSPACE:id}[\\s]+%{NOTSPACE:appname}[\\s]+%{NOTSPACE:cmd}[\\s]+%{ALL:msg}",None)),
    Some(Collector("0", "localhost", 5151)), List(ES2Writer("e1", "elasticsearch", Array(("172.16.219.130",9200)))))
   */
    /* actor ! sysyemOut*/
    case "file-update" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("file0","testSingle",
        SingleChannel((FileSource("data/iislog/SZ_test/", Option("iis"))),
          Some(Regex("%{IIS_LOG}", Array(AddFields(Map(("test", "124"))))))),
        Some(Collector("0", "localhost", 5151)), List(ES2Writer("1", "elasticsearch", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1))), UPDATE)
    case "file-delete" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("file0","testSingle"), DELETE)
    case "file-stop" =>
      actor ! (Config("file0","testSingle"), STOP)
    case "file-start" =>
      actor ! (Config("file0","testSingle"), START)

    case "pipe-create" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      /*actor ! Config("file0",
      */
      /* List(ES2Writer("1", "elasticsearch", Array(("172.16.219.130",9200), ("172.16.219.131",9200)), "logs_%{yyyyMMdd}", "iis_pipe", 2000))*/
      val conf = Config("pipe0","testSingle",
        SingleChannel((NetSource(Some(Akka()), Some("127.0.0.1"), 19093)),
          Some(Delimit(Some("\t"), Array("@timestamp", "Id_Command", "Argument")))),
        Some(Collector("0", "localhost", 5151)), List())
      actor ! (conf, CREATE)

    case "pipe-start" =>
      actor ! (Config("pipe0","testSingle"), START)


    case "es-create" =>
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      val conf = Config("es001","testSingle",

        SingleChannel((ES2Source("elasticsearch", Array(("172.16.219.130",9200), ("172.16.219.131",9200)), "logs_20131210", "logs", "@timestamp", format.parse("2013-12-10 00:00:00.0"), 1000)),
          Some(TransferParser())),
        Some(Collector("0", "localhost", 5151)), List(ES2Writer("1", "elasticsearch", Array(("172.16.219.130",9200), ("172.16.219.131",9200)), "logs_%{yyyyMMdd}", "iis_trans", cache = 1)))

      actor ! (conf, CREATE)
    case "es-stop" =>
      actor ! (Config("es001","testSingle"), STOP)
    case "es-start" =>
      actor ! (Config("es001","testSingle"), START)
    case "hdfs-create" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      val conf = Config("hdfs0","testSingle",
        SingleChannel((HDFSSource("172.16.219.173", "HaimaDev", "nn1", path = FileSource("/log/iis", Option("iis")))),
          Some(Regex("%{IIS_LOG}"))),
        Some(Collector("0", "localhost", 19093)), List(Writer("1", "std")))
      actor ! (conf, CREATE)
    case "hdfs-update" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("hdfs0","testSingle",
        SingleChannel((FileSource("data/iislog/SZ_test/", Option("iis"))),
          Some(Regex("%{IIS_LOG}", Array(AddFields(Map(("test", "124"))))))),
        Some(Collector("0", "localhost", 5151)), List(ES2Writer("1", "elasticsearch", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1))), UPDATE)
    case "hdfs-delete" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      actor ! (Config("hdfs0","testSingle",
        SingleChannel((FileSource("data/iislog/SZ_test/", Option("iis"))),
          Some(Regex("%{IIS_LOG}", Array(AddFields(Map(("test", "124"))))))),
        Some(Collector("0", "localhost", 5151)), List(ES2Writer("1", "elasticsearch", Array(("yzh",9200)), "logs_%{yyyyMMdd}", "iis", cache = 1))), DELETE)
    case "hdfs-stop" =>
      actor ! (Config("hdfs0","testSingle"), STOP)
    case "hdfs-start" =>
      actor ! (Config("hdfs0","testSingle"), START)
    case "ftp-create" =>
      // NetSource("0", Some(Syslog()), "udp", "127.0.0.1", 4301, 1000,  Properties(),
      val conf = Config("ftp0","testSingle",
        SingleChannel((FTPSource("172.16.219.173", Some(21), FileSource("/iislog", Option("iis")), Properties(Map("user" -> "ftpuser", "password" -> "123456")).properties)),
          Some(Regex("%{IIS_LOG}", Array(AddFields(Map(("test", "124"))))))),
        Some(Collector("0", "localhost", 19093)), List(Writer("1", "std")))
      actor ! (conf, CREATE)
    case "ftp-stop" =>
      actor ! (Config("ftp0","testSingle"), STOP)
    case "ftp-start" =>
      actor ! (Config("ftp0","testSingle"), START)



    //actor ! ("0",SelectFlag.CONFIG)
    //查询
    /*case list :List[(Config,CollectorStatus.CStatus,String,DSConfigStatus.ConfigStatus,Status.Status,StreamStatus.StreamStatus,Status.Status)] =>
    list.foreach(
    config =>
    println(config)
    )*/
    case result: Result =>
      logger.info(s"" + result)
  }
}
