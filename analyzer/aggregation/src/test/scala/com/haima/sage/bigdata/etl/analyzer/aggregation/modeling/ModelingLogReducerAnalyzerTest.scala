package com.haima.sage.bigdata.analyzer.aggregation.modeling

import com.haima.sage.bigdata.analyzer.aggregation.model.{LogReducer, Pattern, Terms}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, LogReduceAnalyzer}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.junit.Test

import scala.io.Source
import scala.math.Ordering

/**
  * Created by CaoYong on 2017/11/6.
  */
class ModelingLogReducerAnalyzerTest extends Mapper {
  private val env = ExecutionEnvironment.getExecutionEnvironment
  Constants.init("sage-analyzer-logaggs.conf")

  // val datas1 = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("data.log")).getLines().toArray
  val datas = Array(
//    "[9/7/17 0:31:01:051 CST] 00000046 SystemErr     R log4j:WARN No appenders could be found for logger (org.apache.activemq.broker.BrokerService).",
//    "[9/7/17 0:31:01:051 CST] 00000046 SystemErr     R log4j:WARN Please initialize the log4j system properly.",
//    "[9/7/17 0:31:24:818 CST] 00000095 SystemErr     R java.security.PrivilegedActionException: java.lang.NullPointerException",
//    "[9/7/17 0:31:24:818 CST] 00000095 SystemErr     R 	at com.ibm.ws.security.auth.ContextManagerImpl.runAs(ContextManagerImpl.java:5523)"
//   "警报: 防恶意软件引擎脱机,严重性: 普通",
//  "告警: 123,严重性: 普通",
//  "告警: ado,严重性: 警告",
//  "告警: 防恶意软件引擎脱机1,严重性: 警告",
//  "错误: 防恶意软件引擎脱机1,严重性: 普通",
//  "告警: abc,严重性: 普通",
//  "告警: 防恶意软件引擎脱机2,严重性: 普通",
//  "告警: info,严重性: 普通",
//  "错误: 防恶意软件引擎脱机3,严重性: 警告",
//  "告警: 防恶意软件引擎脱机2,严重性: 警告",
//  "告警: 防恶意软件引擎脱机3,严重性: 普通",
//  "告警: 防恶意软件引擎脱机4,严重性: 普通",
//  "告警: error,严重性: 警告",
//  "告警: 防恶意软件引擎脱机5,严重性: 普通",
//  "告警: 防恶意软件引擎脱机6,严重性: 普通",
//  "告警: 防恶意软件引擎脱机7,严重性: 普通",
//  "警报: 入侵防御引擎脱机,严重性: 严重",
//  "告警: 防火墙引擎脱机,严重性: 严重"
    "我的名字叫aa，今年18岁，是个hao学生",
    "我的名字叫bb，今年18岁，是个xiao孩子",
    "我的名字叫cc，今年18岁，是个huai公务员",
    "我的名字叫dd，今年18岁，是个xiao傻逼",
    "我的名字叫ee，今年18岁，是个学生",
    "我的名字叫ff，今年18岁，是个孩子",
    "我的名字叫gg，今年18岁，是个公务员",
    "我的名字叫hh，今年18岁，是个傻逼",
    "我的名字叫ii，今年18岁，是个da学生",
    "我的名字叫jj，今年18岁，是个da孩子",
    "我的名字叫kk，今年18岁，是个hao公务员",
    "我的名字叫ll，今年18岁，是个da傻逼"
  )

  protected val data: DataSet[RichMap] = env.fromElements(
    datas.zipWithIndex.map {
      case (data, index) =>
        println(data.toString)
       richMap( Map("a" -> "x", "raw" -> data))
    }: _*
  )


  val models = Array(
    """[{"text":"["},{"param":"0"},{"text":"9"},{"param":"1"},{"text":"/"},{"param":"2"},{"text":"7"},{"param":"3"},{"text":"/"},{"param":"4"},{"text":"17"},{"param":"5"},{"text":" "},{"param":"6"},{"text":"0"},{"param":"7"},{"text":":"},{"param":"8"},{"text":"31"},{"param":"9"},{"text":":"},{"param":"10"},{"text":"24"},{"param":"11"},{"text":":"},{"param":"12"},{"text":" "},{"param":"13"},{"text":"CST"},{"param":"14"},{"text":"]"},{"param":"15"},{"text":" "},{"param":"16"},{"text":"00000095"},{"param":"17"},{"text":" "},{"param":"18"},{"text":"SystemErr"},{"param":"19"},{"text":" "},{"param":"20"},{"text":" "},{"param":"21"},{"text":" "},{"param":"22"},{"text":" "},{"param":"23"},{"text":" "},{"param":"24"},{"text":"R"},{"param":"25"},{"text":" "},{"param":"26"},{"text":"	"},{"param":"27"},{"text":"at"},{"param":"28"},{"text":" "},{"param":"29"},{"text":"."},{"param":"30"},{"text":"."},{"param":"31"},{"text":"."},{"param":"32"},{"text":"("},{"param":"33"},{"text":")"},{"param":"34"}]""",
    """[{"text":"["},{"param":"0"},{"text":"9"},{"param":"1"},{"text":"/"},{"param":"2"},{"text":"7"},{"param":"3"},{"text":"/"},{"param":"4"},{"text":"17"},{"param":"5"},{"text":" "},{"param":"6"},{"text":"0"},{"param":"7"},{"text":":"},{"param":"8"},{"text":"31"},{"param":"9"},{"text":":"},{"param":"10"},{"text":"24"},{"param":"11"},{"text":":"},{"param":"12"},{"text":" "},{"param":"13"},{"text":"CST"},{"param":"14"},{"text":"]"},{"param":"15"},{"text":" "},{"param":"16"},{"text":"00000095"},{"param":"17"},{"text":" "},{"param":"18"},{"text":"SystemErr"},{"param":"19"},{"text":" "},{"param":"20"},{"text":" "},{"param":"21"},{"text":" "},{"param":"22"},{"text":" "},{"param":"23"},{"text":" "},{"param":"24"},{"text":"R"},{"param":"25"},{"text":" "},{"param":"26"},{"text":"."},{"param":"27"},{"text":"."},{"param":"28"}]""",
    """[{"text":"["},{"param":"0"},{"text":"9"},{"param":"1"},{"text":"/"},{"param":"2"},{"text":"7"},{"param":"3"},{"text":"/"},{"param":"4"},{"text":"17"},{"param":"5"},{"text":" "},{"param":"6"},{"text":"0"},{"param":"7"},{"text":":"},{"param":"8"},{"text":"31"},{"param":"9"},{"text":":"},{"param":"10"},{"text":"24"},{"param":"11"},{"text":":"},{"param":"12"},{"text":"818"},{"param":"13"},{"text":" "},{"param":"14"},{"text":"CST"},{"param":"15"},{"text":"]"},{"param":"16"},{"text":" "},{"param":"17"},{"text":"00000095"},{"param":"18"},{"text":" "},{"param":"19"},{"text":"SystemErr"},{"param":"20"},{"text":" "},{"param":"21"},{"text":" "},{"param":"22"},{"text":" "},{"param":"23"},{"text":" "},{"param":"24"},{"text":" "},{"param":"25"},{"text":"R"},{"param":"26"},{"text":" "},{"param":"27"},{"text":"	"},{"param":"28"},{"text":"at"},{"param":"29"},{"text":" "},{"param":"30"},{"text":"com"},{"param":"31"},{"text":"."},{"param":"32"},{"text":"ibm"},{"param":"33"},{"text":"."},{"param":"34"},{"text":"."},{"param":"35"},{"text":"."},{"param":"36"},{"text":"."},{"param":"37"},{"text":"("},{"param":"38"},{"text":"."},{"param":"39"},{"text":"java"},{"param":"40"},{"text":":"},{"param":"41"},{"text":")"},{"param":"42"}]""",
    """[{"text":"[9/7/17 0:31:01:051 CST] 00000046 SystemErr     R log4j:WARN No appenders could be found for logger (org.apache.activemq.broker.BrokerService)."}]""",
    """[{"text":"[9/7/17 0:31:01:051 CST] 00000046 SystemErr     R log4j:WARN Please initialize the log4j system properly."}]""",
    """[{"text":"[9/7/17 0:31:24:818 CST] 00000095 SystemErr     R java.security.PrivilegedActionException: java.lang.NullPointerException"}]"""
  )

  protected val model: List[(Pattern, Map[String, Any])] = models.map(m => {
    val map = Map[String, Any]()

    (Pattern(m), map.+("pattern_key" -> "3").+("cluster" -> "3").+("pattern_scale" -> "3"))
  }).toList


  //  @Test
  //  def logaggsModeling(): Unit = {
  //    val conf = ReAnalyzer(Some(LogReduceAnalyzer("", "", Option("raw"), "eventTime", Some(60), Some(60), 0.7)))
  //    val processor = ModelingAnalyzerProcessor(conf)
  //    assert(processor.engine() == "modeling")
  //    processor.process(data).head.map(data => {
  //      (data.get("cluster").orNull, data.get("cluster_total").orNull, data.get("key").orNull, data.get("size").orNull)
  //    }).collect().foreach(println)
  //  }

  @Test
  def logaggsSorted(): Unit = {


    List(1, 3, 2).sortWith((f, s) => f > s).foreach(println)
  }

  @Test
  def logaggsModeling1(): Unit = {
    val conf = LogReduceAnalyzer(Option("raw"), 0.7)
    val modeling = new ModelingLogReducerAnalyzer(conf, AnalyzerType.MODEL)
    modeling.action(data).map(data => {
      (data.get("cluster").orNull, data.get("cluster_total").orNull, data.get("pattern_key").orNull, data.get("pattern_size").orNull)
    }).collect().foreach(println)
  }

  @Test
  def logaggslocal(): Unit = {

    val logReducer: LogReducer = LogReducer(0.6)

    val now = System.currentTimeMillis()
    println("start with :" + now)

    val data = Source.fromFile("/Users/zhhuiyan/Downloads/local-json-test-1000.json").getLines().map(mapper.readValue[Map[String, Any]](_).getOrElse("@msg", "").toString).toList
    println("data:" + data.size)
    val clusters = logReducer.runLogReducer(data)

    println(s"take date:${System.currentTimeMillis() - now}")

    val patterns = clusters.flatMap(_._1)

    data.foreach(d => {
      if (patterns.exists(_.isMatch(Terms(d)))) {
        println("match:", d)
      }
    }
    )
    //    clusters.foreach(
    //      partterns => {
    //
    //        println("total:" + partterns._2)
    //      partterns._1.foreach(pattern => println(pattern.toKey))
    //    })
  }

  @Test
  def logaggsModeling2(): Unit = {
//    val conf = LogReduceAnalyzer(Option("raw"), 0.7, Some("3F73A3E6_5FA2_43D1_A2F1_FB22B4862299"))
//    val modeling = new ModelingLogReducerAnalyzer(conf, AnalyzerType.ANALYZER)
//    modeling.createModels(env.readTextFile("/Users/zhhuiyan/Downloads/message2.txt").map(d => Map[String, Any]("raw" -> d)))
//      .collect().foreach(d => println(d.getOrElse("cluster", ""), d.get("cluster_total").orNull, d.getOrElse("pattern_key", "")))
  }


}
