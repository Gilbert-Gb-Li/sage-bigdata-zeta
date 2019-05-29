package com.haima.sage.bigdata.analyzer.aggregation.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.common.model.{AnalyzerModel, LogReduceAnalyzer}
import com.haima.sage.bigdata.etl.streaming.flink.filter.StreamAnalyzerProcessor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.junit.Test


class LogReducerProcessorTest extends Serializable {


  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(CONF.getInt("flink.parallelism"))
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  Constants.init("sage-analyzer-logaggs.conf")


//  val time = new Date().getTime
  val time = 1510823194860L

//  val datas = Source.fromFile("./target/data.log").getLines().toArray
  val datas: Array[String] = {
    Array(
      "************ Start Display Current Environment ************",
      "Log file started at: [9/7/17 0:27:31:134 CST]",
      "************* End Display Current Environment *************",
      "[9/7/17 0:31:01:051 CST] 00000046 SystemErr     R log4j:WARN No appenders could be found for logger (org.apache.activemq.broker.BrokerService).",
      "[9/7/17 0:31:01:051 CST] 00000046 SystemErr     R log4j:WARN Please initialize the log4j system properly.",
      "[9/7/17 0:31:24:818 CST] 00000095 SystemErr     R java.security.PrivilegedActionException: java.lang.NullPointerException",
      "[9/7/17 0:31:24:818 CST] 00000095 SystemErr     R 	at com.ibm.ws.security.auth.ContextManagerImpl.runAs(ContextManagerImpl.java:5523)",
      "[9/7/17 0:31:24:818 CST] 00000095 SystemErr     R 	at com.ibm.ws.security.auth.ContextManagerImpl.runAsSystem(ContextManagerImpl.java:5603)",
      "[9/7/17 0:31:24:818 CST] 00000095 SystemErr     R 	at com.ibm.ws.security.core.SecurityContext.runAsSystem(SecurityContext.java:255)",
      "[9/7/17 0:31:24:818 CST] 00000095 SystemErr     R 	at com.ibm.iscportal.util.JNDIutil.addObject(JNDIutil.java:139)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.iscportal.portlet.service.ServiceManager.init(ServiceManager.java:215)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.isclite.ISCPlugIn.init(ISCPlugIn.java:65)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at org.apache.struts.action.ActionServlet.initModulePlugIns(Unknown Source)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at org.apache.struts.action.ActionServlet.init(Unknown Source)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at javax.servlet.GenericServlet.init(GenericServlet.java:161)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.servlet.ServletWrapper.init(ServletWrapper.java:344)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.servlet.ServletWrapperImpl.init(ServletWrapperImpl.java:168)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.servlet.ServletWrapper.loadOnStartupCheck(ServletWrapper.java:1368)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.webapp.WebApp.doLoadOnStartupActions(WebApp.java:629)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.webapp.WebApp.commonInitializationFinally(WebApp.java:595)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.webapp.WebAppImpl.initialize(WebAppImpl.java:422)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.webapp.WebGroupImpl.addWebApplication(WebGroupImpl.java:88)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.VirtualHostImpl.addWebApplication(VirtualHostImpl.java:170)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.WSWebContainer.addWebApp(WSWebContainer.java:904)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.WSWebContainer.addWebApplication(WSWebContainer.java:789)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.component.WebContainerImpl.install(WebContainerImpl.java:427)",
      "[9/7/17 0:31:24:819 CST] 00000095 SystemErr     R 	at com.ibm.ws.webcontainer.component.WebContainerImpl.start(WebContainerImpl.java:719)",
      "[9/7/17 0:31:24:820 CST] 00000095 SystemErr     R 	at com.ibm.ws.runtime.component.ApplicationMgrImpl.start(ApplicationMgrImpl.java:1177)"
    )
  }

  protected val data: DataStream[RichMap] = env.fromElements(
    datas.zipWithIndex.map {
      case (da, index) =>
        RichMap(Map("a" -> "x", "raw" -> da, "eventTime" -> new Date(time + index * 1000)))
    }: _*
  )

  @Test
  def windowSlide(): Unit = {

    val conf = ReAnalyzer(Some(LogReduceAnalyzer(Option("raw"), 0.3)))
    val processor = StreamAnalyzerProcessor(conf)
    assert(processor.engine() == AnalyzerModel.STREAMING)
    processor.process(data).head.map(data => {
      (data.get("key").orNull, data.get("total").orNull, data.get("pattern").orNull)
    }).writeAsText("./target/out.txt", FileSystem.WriteMode.OVERWRITE)
    env.execute()
    TimeUnit.SECONDS.sleep(1)

  }
}
