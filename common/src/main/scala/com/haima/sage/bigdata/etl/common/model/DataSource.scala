package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable
import java.util.{UUID, Properties => Props}

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.ProcessFrom.ProcessFrom
import com.haima.sage.bigdata.etl.common.model.filter.Rule
import com.haima.sage.bigdata.etl.common.plugin.Plugin
import com.haima.sage.bigdata.etl.driver._
import com.haima.sage.bigdata.etl.utils.ExceptionUtils
import org.slf4j.LoggerFactory

/**
  * Created by zhhuiyan on 15/4/9.
  */
object DataSource {
  def apply(_name: String,
            _uri: String = ""): DataSource =
    new DataSource() {
      override val name: String = _name
      override val uri: String = _uri
    }

  val writerNamesInfo: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
  val monitorNamesInfo: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
  val previewersInfo: scala.collection.mutable.Map[String, Collector] = scala.collection.mutable.Map[String, Collector]()
}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[SingleTableChannel], name = "single-table"),
  new Type(value = classOf[TupleTableChannel], name = "tuple-table"),
  new Type(value = classOf[TupleChannel], name = "tuple"),
  new Type(value = classOf[SingleChannel], name = "single"),
  new Type(value = classOf[FileSource], name = "file"),
  new Type(value = classOf[NetSource], name = "net"),
  new Type(value = classOf[HDFSSource], name = "hdfs"),
  new Type(value = classOf[KafkaSource], name = "kafka"),
  new Type(value = classOf[RabbitMQSource], name = "rabbitmq"),
  new Type(value = classOf[SFTPSource], name = "sftp"),
  new Type(value = classOf[JDBCSource], name = "jdbc"),
  new Type(value = classOf[AWSSource], name = "aws"),
  new Type(value = classOf[ES6Source], name = "es6"),
  new Type(value = classOf[ES5Source], name = "es5"),
  new Type(value = classOf[ES2Source], name = "es2"),
  new Type(value = classOf[FTPSource], name = "ftp"),
  new Type(value = classOf[SFTPSource], name = "sftp"),
  new Type(value = classOf[PrometheusSource], name = "prometheus")
))
@JsonIgnoreProperties(Array("uri", "timeout"))
trait DataSource extends Serializable {
  val name: String = this.getClass.getSimpleName.toLowerCase.replace("source", "")

  val uri: String

  var timeout: Long = 100

  def runningUpdate: Boolean = false


  def autoRestartForUpdate: Boolean = false

  override def hashCode(): Int =
    this.name.hashCode +
      this.uri.hashCode * 2


  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case parser: DataSource =>
        this.name == parser.name &&
          this.uri == parser.uri
      case _ =>
        false
    }

  }
}


object DataSourceType extends Enumeration {
  type Type = Value
  val Channel: Type = Value("channel")
  val Table: Type = Value("table")
  val NotTable: Type = Value("not-table")
  val ChannelWithoutTable: Type = Value("channel-without-table")
  val Default: Type = Value("default")
}

trait Channel extends DataSource {

  def id: Option[String]

  def withID(id: String): Channel

}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(

  new Type(value = classOf[ParserChannel], name = "parser"),
  new Type(value = classOf[AnalyzerChannel], name = "analyzer")
))
trait ChannelType {
  def name: String
}

case class ParserChannel() extends ChannelType {
  final val name = "parser"
}

case class AnalyzerChannel(cluster: String,
                           jobId: String = "",
                           @JsonScalaEnumeration(classOf[AnalyzerModelType])
                           model: AnalyzerModel.AnalyzerModel = AnalyzerModel.STREAMING,
                           @JsonScalaEnumeration(classOf[AnalyzerTypeTrf])
                           `type`: AnalyzerType.Type = AnalyzerType.ANALYZER
                          ) extends ChannelType {
  final val name = "analyzer"
  final val clusterType: ClusterType.Type = ClusterType.FLINK

  def toMate = FlinkMate(cluster, model, name)


}


case class SingleChannel(dataSource: DataSource,
                         parser: Option[Parser[_ <: Rule]] = None,
                         id: Option[String] = None,
                         collector: Option[Collector] = None,
                         `type`: Option[ChannelType] = Some(ParserChannel())) extends Channel {

  override val name: String = "single"
  override lazy val uri: String = s"single-channel://${id.getOrElse("")}-${UUID.randomUUID().toString}"

  override def withID(id: String): SingleChannel = this.copy(id = Option(id))
}


trait TableChannel extends Channel

case class SingleTableChannel(
                               channel: Channel,
                               table: Table,
                               id: Option[String] = None
                             ) extends TableChannel {

  override val name: String = "single-table"
  override lazy val uri: String = s"single-sql-channel://${channel.uri}-${UUID.randomUUID().toString}"

  override def withID(id: String): TableChannel = this.copy(id = Option(id))
}

case class TupleChannel(first: Channel,
                        second: Channel,
                        on: Join, //None for sql
                        id: Option[String] = None
                       ) extends Channel {
  override val name: String = "tuple"
  override lazy val uri = s"tuple://${first.uri}:${second.uri}"

  override def withID(id: String): TupleChannel = this.copy(id = Option(id))
}

case class TupleTableChannel(first: TableChannel,
                             second: TableChannel,
                             id: Option[String] = None
                            ) extends TableChannel {
  override val name: String = "tuple-table"
  override lazy val uri = s"tuple://${first.uri}:${second.uri}"

  override def withID(id: String): TupleTableChannel = this.copy(id = Option(id))
}

class ProcessFromType extends TypeReference[ProcessFrom.type]


object ProcessFrom extends Enumeration {
  type ProcessFrom = Value
  val START, END = Value
}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[TxtFileType], name = "txt"),
  new Type(value = classOf[WinEvtFileType], name = "winevt"),
  new Type(value = classOf[RCFileType], name = "rc"),
  new Type(value = classOf[ORCFileType], name = "orc"),
  new Type(value = classOf[ExcelFileType], name = "excel"),
  new Type(value = classOf[CSVFileType], name = "csv")

))
trait FileType extends Serializable {
  val name: String = this.getClass.getSimpleName.replace("FileType", "").toLowerCase()

  override def toString = s"FileType($name)"
}

case class TxtFileType() extends FileType {

}

case class WinEvtFileType() extends FileType {

}

case class RCFileType() extends FileType {

}

case class ORCFileType() extends FileType {

}

case class ExcelFileType(header: String, isHaveThead: Boolean = true) extends FileType {
}

case class CSVFileType(header: Option[String] = None, isHaveThead: Boolean = true) extends FileType {
}

case class FileSource(path: String,
                      category: Option[String] = None,
                      contentType: Option[FileType] = None,
                      encoding: Option[String] = None,
                      codec: Option[Codec] = None,
                      @JsonScalaEnumeration(classOf[ProcessFromType])
                      position: Option[ProcessFrom] = None,
                      skipLine: Int = 0) extends DataSource with Plugin with FileMate {
  override val uri: String = path

  override def runningUpdate: Boolean = false

  override def autoRestartForUpdate: Boolean = true


  override def info: String = contentType.get.name

  override def installed(): Boolean = {
    lazy val logger = LoggerFactory.getLogger(classOf[FileSource])
    var bool = true
    try {
      Class.forName(Constants.READERS.getString(contentType.get.name))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.debug("file source install check fail :" + e.getStackTrace.mkString(","))
        bool = false
    }
    bool
  }
}


case class NetSource(protocol: Option[WithProtocol] = Some(Net()),
                     host: Option[String] = None,
                     port: Int = 514,
                     listens: Array[(String, String)] = Array(),
                     codec: Option[Codec] = None,
                     properties: Option[Props] = None)
  extends DataSource with WithProperties with Plugin with NetMate {

  override val uri = s"${protocol.getOrElse("udp")}://${host.getOrElse("0.0.0.0")}:$port"

  override def runningUpdate: Boolean = true

  override def autoRestartForUpdate: Boolean = false

  override def installed(): Boolean = {
    var bool = true
    try {
      val className = protocol.map(_.toString).getOrElse("")
      if (!"".equals(className)) {
        Class.forName(Constants.READERS.getString(className))
      }
    } catch {
      case e: Exception =>
        bool = false
    }
    bool
  }

  override def info: String = protocol.toString

  override val isClient: Boolean = {
    protocol match {
      case Some(_: Akka) =>
        true
      case _ =>
        false
    }
  }
}


case class KafkaSource(
                        hostPorts: String,
                        topic: Option[String],
                        codec: Option[Codec] = None,
                        wildcard: String = "false",
                        @JsonScalaEnumeration(classOf[ProcessFromType])
                        position: Option[ProcessFrom] = None,
                        @JsonScalaEnumeration(classOf[AuthTypeType])
                        override val authentication: Option[AuthType.AuthType] = None,
                        properties: Option[Props] = None,
                        groupId: String = "")
  extends KafkaMate with DataSource with WithProperties with Plugin {
  override lazy val uri = s"kafka://$hostPorts/${get("group_id", "")}/${get("client_id", "")}"

  override def runningUpdate: Boolean = true

  override def autoRestartForUpdate: Boolean = false

  override def info: String = name

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case e: Exception =>
        bool = false
    }
    bool
  }

  def withGroupId(id: String) = {
    this.copy(groupId = id)
  }

}

case class RabbitMQSource(
                           host: String = "127.0.0.1",
                           port: Option[Int] = None,
                           virtualHost: Option[String] = None,
                           queue: String = "endpoint",
                           durable: Boolean = true,
                           username: Option[String] = None,
                           password: Option[String] = None,
                           encoding: Option[String] = None) extends RabbitMQMate with DataSource with Plugin {
  override val uri = s"amqp://$host:${port.getOrElse(5672)}/$queue"

  override def info: String = name

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case e: Exception =>
        bool = false
    }
    bool
  }
}

trait WithPath extends DataSource with Plugin {

  def path: FileSource

  private var _info = name

  def info: String = _info

  //  override var info: String = name
  override def installed(): Boolean = {
    lazy val logger = LoggerFactory.getLogger(classOf[HDFSSource])
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
      bool = path.installed()
      _info = path.info
    } catch {
      case e: Exception =>
        logger.debug(s"plugin [$name] not found, cause: " + ExceptionUtils.getMessage(e))
        bool = false
    }
    bool
  }
}


//hdfs没有reader,所以source没有实现Plugin
case class HDFSSource(// host: String,
                      // port: Int,
                      override val nameService: String,
                      override val nameNodes: String,
                      override val host: String,
                      @JsonScalaEnumeration(classOf[AuthTypeType])
                      override val authentication: Option[AuthType.AuthType] = Some(AuthType.KERBEROS),
                      path: FileSource
                     ) extends HDFSMate with WithPath {
  override val uri = s"hdfs://$nameService/${path.uri}"

  override def runningUpdate: Boolean = false

  override def autoRestartForUpdate: Boolean = true

}

case class FTPSource(host: String,
                     port: Option[Int] = Some(21),
                     path: FileSource,
                     properties: Option[Props] = None
                    ) extends FTPMate with WithPath with WithProperties {
  override val uri = s"ftp://$host:${port.get}/${path.uri}"

  override def runningUpdate: Boolean = false

  override def autoRestartForUpdate: Boolean = true


}

//sftp没有reader,所以source没有实现Plugin
case class SFTPSource(host: String,
                      port: Option[Int],
                      path: FileSource,
                      properties: Option[Props] = None
                     ) extends SFTPMate with WithPath with WithProperties {

  override val uri = s"sftp://$host:$port/${path.uri}"

  override def runningUpdate: Boolean = false

  override def autoRestartForUpdate: Boolean = true

}

case class TupleSource(first: DataSource,
                       second: DataSource
                      ) extends DataSource with Plugin {
  override val uri = s"tuple://${first.uri}:${second.uri}"

  override def info: String = name
}

@JsonIgnoreProperties(Array("table"))
case class JDBCSource(protocol: String,
                      driver: String,
                      host: String = "0.0.0.0",
                      port: Int = 0,
                      schema: String,
                      tableOrSql: String,
                      column: String,
                      start: Any,
                      step: Long,
                      properties: Option[Props] = None
                     ) extends JDBCMate with DataSource with WithProperties with Plugin {


  override val uri = s"$protocol://$host:$port/$schema/${if (tableOrSql.length > 50) tableOrSql.hashCode.abs else tableOrSql.replaceAll("[ */\\()]", "")}/$column"

  override def runningUpdate: Boolean = true

  override def autoRestartForUpdate: Boolean = true

  override lazy val table: String = tableOrSql

  override def info: String = protocol

  override def installed(): Boolean = {
    lazy val logger = LoggerFactory.getLogger(classOf[HDFSSource])
    var bool = true
    try {
      Constants.CONF.getConfig("app.protocol").getString(protocol) != null
    } catch {
      case e: Exception =>
        logger.warn(s" jdbc driver is not find" + e)
        bool = false
    }
    bool
  }

  lazy val SELECT: String = {
    if (table.trim.contains(" ")) {
      protocol match {
        case "sybase" =>
          if (table.contains("?")) s"$table by $column asc"
          else if (table.contains("where"))
            s"$table  where $column> ? and $column<= ? order by $column asc"
          else
            s"$table  and $column> ? and $column<= ? order by $column asc"
        case _ =>
          if (table.contains("?")) s"SELECT * FROM ($table) as tmp order by tmp.$column asc"
          else s"SELECT * FROM ($table) as tmp where $column> ? and $column<= ? order by tmp.$column asc"
      }
    } else
      s"SELECT * FROM $table where $column> ? and $column<= ? order by $column asc"
  }

  lazy val SELECT_FOR_META: String = {
    if (table.trim.contains(" ")) {
      protocol match {
        case "sybase" =>
          if (table.contains("?")) s"$table by $column asc where 1=2"
          else if (table.contains("where"))
            s"$table and 1=2"
          else
            s"$table where 1=2"
        case _ =>
          if (table.contains("?")) s"SELECT * FROM ($table) as tmp where 1=2"
          else s"SELECT * FROM ($table) as tmp where 1=2"
      }
    } else
      s"SELECT * FROM $table where 1=2"
  }

  lazy val MAX: String = {
    if (table.trim.contains(" ")) {
      protocol match {
        case "sybase" =>
          if (table.contains("?")) s"$table by $column asc where 1=2"
          else if (table.contains("where"))
            s"$table and 1=2"
          else
            s"$table where 1=2"
        case _ =>
          s"SELECT max(tmp.$column) FROM ($table) as tmp"
      }
    }
    else s"SELECT max($column) FROM $table"
  }
  lazy val MIN: String = {
    if (table.trim.contains(" ")) {
      s"SELECT min(tmp.$column) FROM ($table) as tmp"
    }
    else s"SELECT min($column) FROM $table"
  }
}

object JDBCSource {
  val protocolNamesInfo: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
}

case class ES6Source(cluster: String,
                     hostPorts: Array[(String, Int)],
                     index: String = "logs_%{yyyyMMdd}",
                     esType: String = "logs",
                     field: String,
                     start: Any,
                     step: Long,
                     size: Int,
                     queryDSL: Option[String]
                    ) extends ElasticSearchMate with DataSource with Plugin {
  override val uri: String = s"$cluster://${hostPorts.map(data => data._1 + ":" + data._2).mkString(",")}:$index/$esType/$field"

  override def runningUpdate: Boolean = true

  override def info: String = name

  override def autoRestartForUpdate: Boolean = true

  override def installed(): Boolean = {
    lazy val logger = LoggerFactory.getLogger(classOf[ES5Source])
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case e: Exception =>
        logger.debug("elastic plugin not install " + e.getStackTrace.mkString(","))
        bool = false
    }
    bool
  }
}

case class ES5Source(cluster: String,
                     hostPorts: Array[(String, Int)],
                     index: String = "logs_%{yyyyMMdd}",
                     esType: String = "logs",
                     field: String,
                     start: Any,
                     step: Long,
                     queryDSL: Option[String]
                    ) extends ElasticSearchMate with DataSource with Plugin {
  override val uri: String = s"$cluster://${hostPorts.map(data => data._1 + ":" + data._2).mkString(",")}:$index/$esType/$field"

  override def runningUpdate: Boolean = true

  override def info: String = name

  override def autoRestartForUpdate: Boolean = true

  override def installed(): Boolean = {
    lazy val logger = LoggerFactory.getLogger(classOf[ES5Source])
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case e: Exception =>
        logger.debug("elastic plugin not install " + e.getStackTrace.mkString(","))
        bool = false
    }
    bool
  }
}


case class ES2Source(cluster: String,
                     hostPorts: Array[(String, Int)],
                     index: String = "logs_%{yyyyMMdd}",
                     esType: String = "logs",
                     field: String,
                     start: Any,
                     step: Long
                    ) extends ElasticSearchMate with DataSource with Plugin {
  override val uri: String = s"$cluster://${hostPorts.map(data => data._1 + ":" + data._2).mkString(",")}:$index/$esType/$field"

  override def runningUpdate: Boolean = true

  override def info: String = name

  override def autoRestartForUpdate: Boolean = true

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case e: Exception => {
        bool = false
      }
    }
    bool
  }


}

case class AWSSource(accessKey: Option[String] = None,
                     secretKey: Option[String] = None,
                     region: Option[String] = Some("cn-north-1"),
                     servers: Array[String],
                     start: Any) extends DataSource with Plugin {
  override val uri: String = s"aws:$region/${servers.mkString(",")}/$start"

  override def runningUpdate: Boolean = true


  override def autoRestartForUpdate: Boolean = true

  override def info: String = name

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case e: Exception =>
        bool = false
    }
    bool
  }
}

case class FlinkStreamSource(flink: String,
                             host: String,
                             port: Int,
                             system: String = "worker",
                             app: String = "publisher",
                             handler: Analyzer,
                             window: Int = 2)
  extends DataSource with Plugin {
  override val name = "stream.flink"
  override val uri = s"$flink#$system@$host:$port/$app?handler=$handler&window=$window"

  override def runningUpdate: Boolean = false

  override def autoRestartForUpdate: Boolean = false

  override def info: String = name

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case e: Exception => {
        bool = false
      }
    }
    bool
  }
}

//@SerialVersionUID(-6559104034926051478L)
case class PrometheusSource(host: String = "0.0.0.0",
                            port: Int = 0,
                            expression: String,
                            start: Long,
                            step: Long = 1
                            , rangeStep: Long = 1000,
                            properties: Option[Props] = None) extends PrometheusMate with DataSource with WithProperties with Plugin {
  override val uri: String = s"http://$host:$port/api/v1/query_range?query=$expression&start=$start&end=${start + rangeStep}&step=${step}s"

  override def runningUpdate: Boolean = true

  override def autoRestartForUpdate: Boolean = true

  override def info: String = name

}