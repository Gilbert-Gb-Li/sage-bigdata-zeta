package com.haima.sage.bigdata.etl.common

import java.io._
import java.util.concurrent.Executors

import com.haima.sage.bigdata.etl.common.base.Lexer
import com.haima.sage.bigdata.etl.utils.{ClassUtils, Logger}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:05.
  */
object Constants extends Logger {

  class Constants

  implicit val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))
  private[Constants] val loader: ClassLoader = classOf[Constants].getClassLoader

  private[Constants] val CLASSPATH = loader.getClass.getResource("/").getPath
  val ID: String = "worker.id"
  val PROCESS_SIZE: String = "worker.process.size"
  val PROCESS_SIZE_DEFAULT: Int = 10

  //metric
  val METRIC_INTERVAL_SECOND: String = "worker.metric.interval.second"
  val METRIC_INTERVAL_MINUTE = "worker.metric.interval.minute"
  val METRIC_INTERVAL_HOUR = "worker.metric.interval.hour"
  val METRIC_INTERVAL_DAY = "worker.metric.interval.day"
  val METRIC_SWITCH_TABLE_MIN = "worker.metric.table.min"
  val METRIC_SWITCH_TABLE_MAX = "worker.metric.table.max"
  val METRIC_EXTRACT_BASE_FIELD = "worker.metric.extract.field"
  val METRIC_SAVE_DAYS = "worker.metric.save.days"

  //file relation
  val READER_FILE_BUFFER: String = "app.reader.file.buffer"
  val READER_FILE_BUFFER_DEFAULT: Int = 10000

  //thread
  val PROCESS_CACHE_SIZE: String = "worker.process.cache.size"
  val PROCESS_CACHE_SIZE_DEFAULT: Long = 100000
  val PROCESS_WAIT_TIMES: String = "worker.process.wait.times"
  val PROCESS_WAIT_TIMES_DEFAULT: Long = 500


  //Zookeeper kafka
  val ZOOKEEPER_SESSION_TIMEOUT_MS: String = "zookeeper.session.timeout.ms"
  val ZOOKEEPER_CONNECTION_TIMEOUT: String = "zookeeper.connection.timeout.ms"
  val AUTO_OFFSET_RESET: String = "auto.offset.reset"

  val CLUSTER_CONNECT_TIMEOUT: Int = 300 //DescribeClusterOptions().timeoutMs(mix=300)

  val PROCESS_THREAD_LEXER_SIZE: String = "worker.process.lexer.size"
  val PROCESS_THREAD_LEXER_SIZE_DEFAULT: Int = 1000


  val ADD_RAW_ATTR: String = "worker.add.raw.attr"
  val ADD_RAW_ATTR_DEFAULT: Boolean = false
  val ADD_RAW_DATA: String = "worker.add.raw.data"
  val ADD_RAW_DATA_DEFAULT: Boolean = false
  val ADD_COLLECTOR_INFO: String = "worker.add.collector.info"
  val ADD_COLLECTOR_INFO_DEFAULT: Boolean = false
  val ADD_RECEIVE_TIME: String = "worker.add.receive.time"
  val ADD_RECEIVE_TIME_DEFAULT: Boolean = false
  val ADD_SOURCE_INFO: String = "worker.add.source.info"
  val ADD_SOURCE_INFO_DEFAULT: Boolean = false

  val ADD_CLUSTER_INFO: String = "worker.add.cluster.info"
  val ADD_CLUSTER_INFO_DEFAULT: Boolean = false
  val ADD_ANALYZE_TIME: String = "worker.add.analyze.time"
  val ADD_ANALYZE_TIME_DEFAULT: Boolean = false

  val REMOTE: String = "worker.remote"
  val STORE_POSITION_FLUSH_TYPES = Array("positive", "negative")
  val STORE_POSITION_FLUSH: String = "app.store.position.flush"
  val STORE_POSITION_FLUSH_DEFAULT: String = STORE_POSITION_FLUSH_TYPES(0)
  val STORE_POSITION_OFFSET: String = "app.store.position.offset"
  val STORE_POSITION_OFFSET_DEFAULT: Int = 5000

  val STORE_POSITION_CLASS: String = "app.store.position.class"
  val STORE_CACHE_CLASS: String = "app.store.cache.class"
  val STORE_CACHE_CLASS_DEFAULT: String = "com.haima.sage.bigdata.etl.store.cache.DerbyStore"
  val STORE_STATUS_CLASS: String = "app.store.status.class"
  val STORE_COLLECTOR_CLASS: String = "app.store.collector.class"
  val STORE_ANALYZER_CLASS: String = "app.store.analyzer.class"
  val STORE_MODELING_CLASS: String = "app.store.modeling.class"
  val STORE_CONFIG_CLASS: String = "app.store.config.class"
  val STORE_DATASOURCE_CLASS: String = "app.store.datasource.class"
  val STORE_METRIC_INFO_CLASS: String = "app.store.metric.info.class"
  val STORE_METRIC_HISTORY_CLASS: String = "app.store.metric.history.class"
  val STORE_RULE_UNIT_CLASS: String = "app.store.rule.unit.class"
  val STORE_RULE_GROUP_CLASS: String = "app.store.rule.group.class"
  val STORE_PARSER_CLASS = "app.store.parser.class"
  val STORE_ASSET_TYPE_CLASS = "app.store.assettype.class"
  val ASSETS_CLASS = "app.store.asset.class"
  val WRITER_CLASS = "app.store.writer.class"
  val TIMER_CLASS = "app.store.task.class"
  val TIMER_MSG_CLASS = "app.store.taskloginfo.class"
  val DICTIONARY_CLASS = "app.store.dictionary.class"
  val ADD_ASSET_DATA: String = "dictionary.add.asset.data"
  val RULE_BANK_FILE_PATH: String = "master.rule.bank.file.path"
  val HTML_DOC_PATH: String = "master.html.doc.path"
  val SECURITY_KEY_STORE_PATH: String = "master.https.key-store-path"
  val SECURITY_KEY_STORE_PASSWORD: String = "master.https.key-store-password"
  val MASTER_SERVICE_TIMEOUT: String = "master.timeout"

  val KNOWLEDGE_CLASS: String = "app.store.knowledge.class"

  val KNOWLEDGE_INFO_CLASS: String = "app.store.knowledgeInfo.class"

  val MODELING_PROCESSOR_CLASS = "app.modeling.processor"

  val MODELING_PREVIEWER_CLASS = "app.modeling.previewer"

  val FLINK_WATCHER_CLASS = "app.flink.watcher"

  val STREAMING_PROCESSOR_CLASS = "app.lexer.flink"

  private var _conf = {
    //val conf = ConfigFactory.parseResourcesAnySyntax("worker.conf")

    val path = new File(CLASSPATH)

    /**
      * merge all plugins
      */
    /*if (path.listFiles().exists(_.getName == "worker.conf")) {
      try {
        val worker = ConfigFactory.parseResourcesAnySyntax("worker.conf")
        worker.getString(Constants.ID) match {
          case "worker" =>
            val reader = new BufferedReader(new FileReader(s"$path/worker.conf"))
            val streams = reader.lines()
            var d = List[String]()
            streams.forEach(new Consumer[String] {
              override def accept(t: String): Unit = {
                d = d.+:(t)
              }
            })
            reader.close()
            val file = new File(s"$path/worker.conf")
            val stream = new FileOutputStream(file, false)
            stream.write(s"${Constants.ID} = ${UUID.randomUUID().toString}\n".getBytes)
            d.filter(!_.contains(Constants.ID)) foreach (l => stream.write((l + "\n").getBytes))
            stream.flush()
            stream.close()
          case _ =>
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }

    }*/

    val configs = path.listFiles().filter(file => {
      file.getName.endsWith("conf") &&
        file.getName != "application.conf" &&
        file.getName != "master.conf" &&
        file.getName != "worker.conf" &&
        file.getName != "daemon.conf" &&
        file.getName != "auth.conf"
    }).map(f => {
      logger.debug(s"check:config file[${f.getName}]")
      val con = ConfigFactory.parseFile(f)
      con
    })
    if (configs.isEmpty) {
      None
    } else {
      Some(configs.reduce((left, right) =>
        left.withFallback(right))) //.withFallback(ConfigFactory.load("application.conf"))

    }


  }

  final def CONF: Config = {
    _conf.getOrElse(ConfigFactory.load("application.conf"))
  }


  def init(name: String): Unit = {

    _conf = Option(ConfigFactory.parseResourcesAnySyntax(name).withFallback(_conf.map(_.withFallback(ConfigFactory.load("application.conf"))).getOrElse(ConfigFactory.load("application.conf"))))
  }

  final lazy val CODECS = CONF.getConfig("app.codec")
  final lazy val READERS = CONF.getConfig("app.reader")
  final lazy val MONITORS = CONF.getConfig("app.monitor")
  final lazy val WRITERS = CONF.getConfig("app.writer")
  final lazy val connectWithCluster: Boolean = Try(CONF.getString("worker.connect-to").matches("cluster")).getOrElse(true)

  final lazy val MASTER_API = CONF.getConfig("api")
  final lazy val MASTER = CONF.getConfig("master")
  final lazy val LEXERS = ClassUtils.subClass(classOf[Lexer[_,_]],params = 3)

  def getApiServerConf(name: String): String = {
    CONF.getString(name)
  }


}

