package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable
import java.util.{UUID, Properties => Props}

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.SwitchType.SwitchType
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model.writer.{ContentType, JDBCField, Json, NameType}
import com.haima.sage.bigdata.etl.common.plugin.Plugin
import com.haima.sage.bigdata.etl.driver._
import com.haima.sage.bigdata.etl.utils.Logger

/**
  * Created by zhhuiyan on 15/4/9.
  */
object Writer {
  def apply(_id: String = UUID.randomUUID().toString, _name: String, _cache: Int = 1000) = new Writer {
    override val id: String = _id
    override val name: String = _name
    override val cache: Int = _cache

    override def uri: String = name
  }
}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name", visible = true)
@JsonSubTypes(Array(
  new Type(value = classOf[ES2Writer], name = "es2"),
  new Type(value = classOf[ES5Writer], name = "es5"),
  new Type(value = classOf[ES6Writer], name = "es6"),
  new Type(value = classOf[StdWriter], name = "std"),
  new Type(value = classOf[JDBCWriter], name = "jdbc"),
  new Type(value = classOf[NetWriter], name = "net"),
  new Type(value = classOf[ForwardWriter], name = "forward"),
  new Type(value = classOf[FileWriter], name = "file"),
  new Type(value = classOf[KafkaWriter], name = "kafka"),
  new Type(value = classOf[RabbitMQWriter], name = "rabbitmq"),
  new Type(value = classOf[SyslogWriter], name = "syslog"),
  new Type(value = classOf[HDFSWriter], name = "hdfs"),
  new Type(value = classOf[SwitchWriter], name = "switch"),
  new Type(value = classOf[RestfulWriter], name = "restful")
))
@JsonIgnoreProperties(Array("logger", "loader", "loggerName", "uri"))
trait Writer extends Serializable {
  val id: String = UUID.randomUUID().toString
  val name: String = this.getClass.getSimpleName.toLowerCase.replace("writer", "")
  val cache: Int = 1000

  def uri: String


  override def hashCode(): Int = id.hashCode +
    /*name.hashCode * 2 +*/
    cache * 4

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case writer: Writer =>
        this.name == writer.name &&
          this.id == writer.id &&
          this.cache == writer.cache
      case _ =>
        false
    }
  }

  def setId(id: String): Writer = this
}


class SwitchTypeRef extends TypeReference[SwitchType.type]


object SwitchType extends Enumeration {
  type SwitchType = Value
  val Regex, StartWith, EndWith, Match, Contain = Value
}


case class SwitchWriter(override val id: String = UUID.randomUUID().toString,
                        field: String,
                        @JsonScalaEnumeration(classOf[SwitchTypeRef])
                        `type`: SwitchType,
                        writers: Array[(String, Writer)], default: Writer) extends Writer with Plugin {
  override val uri: String = s"switch://$field@${`type`.toString}/${writers.map(path => path._1 + "=>" + path._2.uri).mkString("&")}||default=>${default.uri}"

  override def info: String = ???

  override def setId(_id: String): Writer = this.copy(id = _id)
}

//id/中文名称/时间/排序/data
case class StdWriter(override val id: String = UUID.randomUUID().toString,
                     contentType: Option[ContentType] = None) extends Writer with Plugin with WriteUsable {
  override val uri: String = s"std"

  override def info: String = name

  override def setId(_id: String): Writer = this.copy(id = _id)
}


case class ES2Writer(override val id: String = UUID.randomUUID().toString,

                     cluster: String,
                     hostPorts: Array[(String, Int)],
                     index: String = "logs_$",
                     indexType: String = "logs",
                     routingField: Option[String] = None,
                     parentField: Option[String] = None,
                     asChild: Option[String] = None,
                     idFields: Option[String] = None,
                     number_of_shards: Int = 5,
                     number_of_replicas: Int = 0,
                     numeric_detection: Boolean = false,
                     date_detection: Boolean = false,
                     enable_size: Boolean = false,
                     persisRef: Option[NameType] = None,
                     metadata: Option[List[(String, String, String)]] = None,
                     override val name: String = "es-2",
                     override val cache: Int = 1000) extends ElasticSearchMate with Writer with Plugin {
  override val uri: String = s"elasticsearch://$cluster@${hostPorts.map(add => add._1 + ":" + add._2).mkString(",")}/$index/$indexType"

  override def info: String = name

  override def setId(_id: String): Writer = this.copy(id = _id)
}

case class ES6Writer(override val id: String = UUID.randomUUID().toString,

                     cluster: String,
                     hostPorts: Array[(String, Int)],
                     index: String = "logs_$",
                     indexType: String = "logs",
                     routingField: Option[String] = None,
                     parentField: Option[String] = None,
                     asChild: Option[String] = None,
                     idFields: Option[String] = None,
                     isScript: Option[Boolean] = None,
                     script: Option[String] = None,
                     number_of_shards: Int = 5,
                     number_of_replicas: Int = 0,
                     numeric_detection: Boolean = false,
                     date_detection: Boolean = false,
                     enable_size: Boolean = false,
                     persisRef: Option[NameType] = None,
                     metadata: Option[List[(String, String, String)]] = None,
                     override val name: String = "es-2",
                     override val cache: Int = 1000) extends ElasticSearchMate with Writer with Plugin {
  override val uri: String = s"elasticsearch://$cluster@${hostPorts.map(add => add._1 + ":" + add._2).mkString(",")}/$index/$indexType"

  override def info: String = name

  override def setId(_id: String): Writer = this.copy(id = _id)

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.WRITERS.getString(name))
    } catch {
      case _: Exception =>
        bool = false
    }
    bool
  }
}

case class ES5Writer(override val id: String = UUID.randomUUID().toString,

                     cluster: String,
                     hostPorts: Array[(String, Int)],
                     index: String = "logs_$",
                     indexType: String = "logs",
                     routingField: Option[String] = None,
                     parentField: Option[String] = None,
                     asChild: Option[String] = None,
                     idFields: Option[String] = None,
                     number_of_shards: Int = 5,
                     number_of_replicas: Int = 0,
                     numeric_detection: Boolean = false,
                     date_detection: Boolean = false,
                     enable_size: Boolean = false,
                     persisRef: Option[NameType] = None,
                     metadata: Option[List[(String, String, String)]] = None,
                     override val name: String = "es-2",
                     override val cache: Int = 1000) extends ElasticSearchMate with Writer with Plugin {
  override val uri: String = s"elasticsearch://$cluster@${hostPorts.map(add => add._1 + ":" + add._2).mkString(",")}/$index/$indexType"

  override def info: String = name

  override def setId(_id: String): Writer = this.copy(id = _id)

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.WRITERS.getString(name))
    } catch {
      case _: Exception =>
        bool = false
    }
    bool
  }
}

case class JDBCWriter(override val id: String = UUID.randomUUID().toString,
                      protocol: String,
                      driver: String,
                      host: String = "0.0.0.0",
                      port: Int = 0,
                      schema: String,
                      table: String,
                      column: String,
                      persisRef: Option[NameType] = None,
                      metadata: Option[List[JDBCField]] = None,
                      properties: Option[Props] = None,
                      override val cache: Int = 1000) extends JDBCMate with Writer with WithProperties with Plugin with Logger {
  override val uri: String = s"$protocol://$host:$port/$schema/$table"

  override def installed(): Boolean = {
    var bool = true
    try {
      Constants.CONF.getConfig("app.protocol").getString(protocol) != null
    } catch {
      case e: Exception =>
        logger.error(s"plugin not install : ${e.printStackTrace()}")
        bool = false
    }
    bool
  }

  override def info: String = protocol

  override def setId(_id: String): Writer = this.copy(id = _id)
}

//这个和source暂时参数格式不一致，待忠辉处理后，再添加
case class NetWriter(override val id: String = UUID.randomUUID().toString,
                     protocol: Option[Protocol] = Some(UDP()),
                     host: Option[String] = None,
                     port: Int = 514,
                     contentType: Option[ContentType] = None,
                     override val cache: Int = 1000) extends Writer with WriteUsable with NetMate {
  override val uri: String = s"net://$protocol@$host:$port"

  override def isClient = true

  override def setId(_id: String): Writer = this.copy(id = _id)
}

//这个暂时不用，后续可能要去掉的功能
case class SyslogWriter(override val id: String = UUID.randomUUID().toString,
                        protocol: Option[Protocol] = Some(UDP()),
                        host: Option[String] = None,
                        port: Int = 514,
                        contentType: Option[ContentType] = None,
                        override val cache: Int = 1000) extends Writer with NetMate with WriteUsable {
  override val uri: String = s"syslog:$protocol//@$host:$port/${contentType.map(_.toString).getOrElse("")}"

  override def isClient = true

  override def setId(_id: String): Writer = this.copy(id = _id)
}

case class KafkaWriter(override val id: String = UUID.randomUUID().toString,
                       hostPorts: String,
                       topic: Option[String] = Option("topic"),
                       contentType: Option[ContentType] = None,
                       @JsonScalaEnumeration(classOf[AuthTypeType])
                       override val authentication: Option[AuthType.AuthType] = None,
                       persisRef: Option[NameType] = None,
                       properties: Option[Props] = None,
                       override val cache: Int = 1000) extends KafkaMate with Writer with Plugin with WriteUsable {
  override val uri: String = s"kafka://$hostPorts/${topic.getOrElse("")}"

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case _: Exception =>
        bool = false
    }
    bool
  }

  override def info: String = name

  override def setId(_id: String): Writer = this.copy(id = _id)
}

case class ForwardWriter(override val id: String = UUID.randomUUID().toString,
                         host: String,
                         port: Int,
                         system: Option[String] = Some("worker"),
                         app: Option[String] = Some("/user/publisher"),
                         identifier: Option[String] = None,
                         persisRef: Option[NameType] = None,
                         override val cache: Int = 1000) extends Writer {
  override val uri: String = s"akka://${system.getOrElse("worker")}@$host:$port/$app"

  override def setId(_id: String): Writer = this.copy(id = _id)
}

case class RabbitMQWriter(override val id: String = UUID.randomUUID().toString,
                          host: String = "127.0.0.1",
                          port: Option[Int] = None,
                          virtualHost: Option[String] = None,
                          queue: String = "endpoint",
                          durable: Boolean = true,
                          username: Option[String] = None,
                          password: Option[String] = None,
                          persisRef: Option[NameType] = None,
                          contentType: Option[ContentType] = None,
                          override val cache: Int = 1000) extends RabbitMQMate with Writer with Plugin with WriteUsable {
  override val uri: String = s"RabbitMQ://@$host:${port.getOrElse("5167")}/${virtualHost.getOrElse("")}/$queue"

  override def info: String = name

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case _: Exception =>
        bool = false
    }
    bool
  }

  override def setId(_id: String): Writer = this.copy(id = _id)
}

case class FtpWriter(override val id: String = UUID.randomUUID().toString,
                     host: String = "127.0.0.1",
                     port: Option[Int] = None,
                     path: FileWriter,
                     properties: Option[Props] = None) extends FTPMate with Writer with Plugin with WriteUsable {
  override val uri: String = s"ftp://@$host:${port.getOrElse("21")}/${path.uri}"

  override def info: String = name

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case _: Exception =>
        bool = false
    }
    bool
  }

  override def contentType: Option[ContentType] = path.contentType

  override def setId(_id: String): Writer = this.copy(id = _id)
}

case class SFtpWriter(override val id: String = UUID.randomUUID().toString,
                      host: String = "127.0.0.1",
                      port: Option[Int] = None,
                      path: FileWriter,
                      properties: Option[Props] = None) extends SFTPMate with Writer with Plugin with WriteUsable {
  override val uri: String = s"sftp://@$host:${port.getOrElse("22")}/${path.uri}"

  override def info: String = name

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case _: Exception =>
        bool = false
    }
    bool
  }

  override def contentType: Option[ContentType] = path.contentType

  override def setId(_id: String): Writer = this.copy(id = _id)
}

case class HDFSWriter(override val id: String = UUID.randomUUID().toString,
                      override val host: String = "127.0.0.1",
                      // port: Int = 9000,
                      override val nameService: String,
                      override val nameNodes: String,
                      path: FileWriter,
                      @JsonScalaEnumeration(classOf[AuthTypeType])
                      override val authentication: Option[AuthType.AuthType] = Some(AuthType.KERBEROS),
                      properties: Option[Props] = None) extends HDFSMate with Writer with Plugin with WriteUsable {
  override val uri: String = s"hdfs://$nameService/${path.uri}"

  override def info: String = name

  override def installed(): Boolean = {
    var bool = true
    try {
      Class.forName(Constants.MONITORS.getString(name))
    } catch {
      case _: Exception =>
        bool = false
    }
    bool
  }

  override def contentType: Option[ContentType] = path.contentType

  override def setId(_id: String): Writer = this.copy(id = _id)
}

//这个和fileSource参数类型不一致，暂时没有添加plugin的验证
case class FileWriter(override val id: String = UUID.randomUUID().toString,
                      path: String,
                      contentType: Option[ContentType] = None,
                      persisRef: Option[NameType] = None,
                      override val cache: Int = 1000) extends Writer with FileMate with WriteUsable {
  override val uri: String = s"file://$path"

  override def setId(_id: String): Writer = this.copy(id = _id)
}

case class RestfulWriter(override val id: String = UUID.randomUUID().toString,
                         protocol: Option[String] = Some("http"),
                         host: String = "127.0.0.1",
                         port: Int = 9000,
                         path: String = "",
                         contentType: Option[ContentType] = Some(new Json)

                        ) extends Writer with WriteUsable with Plugin {
  override val uri: String = s"${protocol.get}://$host:$port/$path"

  override def info: String = name

  override def setId(_id: String): Writer = this.copy(id = _id)
}

trait WriteUsable {

  def contentType: Option[ContentType]

  def writeable(parser: Parser[MapRule]): Usability = {

    contentType match {
      case Some(delimit: writer.Delimit) =>
        val fieldList = delimit.fields.get.split(delimit.delimit match { case None => "," case Some(d) => d })
        val metadataList = parser match {
          case delimit: Delimit => delimit.fields.toList
          case _ => parser.metadata match {
            case None => List[String]()
            case Some(metadata) => for (tuple <- metadata) yield tuple._1
          }
        }
        var flag = false //是否有不匹配的字段，默认是没有  false
      val notMatchFieldsBuf = new StringBuffer()
        fieldList.map(field =>
          if (!metadataList.contains(field)) {
            flag = true
            notMatchFieldsBuf.append(field).append(" ")
          }
        )
        if (flag) {
          val msg = s"${notMatchFieldsBuf.toString} is not in metaList of configuration parsing rules "
          Usability(usable = false, cause = msg)
        }
        else
          Usability()
      case _ =>
        Usability()
    }
  }

}