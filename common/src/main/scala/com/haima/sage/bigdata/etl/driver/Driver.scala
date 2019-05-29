package com.haima.sage.bigdata.etl.driver

import java.util.Properties

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{AnalyzerModel, Authorization, Protocol}
import com.haima.sage.bigdata.etl.common.plugin.{Checkable, Plugin}

import scala.util.Try

/**
  * Created by zhhuiyan on 2017/4/17.
  */
trait Driver[T] {
  val mate: DriverMate

  def driver(): Try[T]
}

trait DriverMate extends Checkable {
  val name: String

  def uri: String

  def usable(): Try[Class[_]] = Try(Class.forName(Constants.CONF.getConfig("app.checker").getString(this.name)))
}

case class FlinkMate(cluster: String, model: AnalyzerModel.AnalyzerModel = AnalyzerModel.STREAMING, override val name: String) extends DriverMate with Plugin {
  protected var _info: String = "flink"

  override def usable(): Try[Class[_]] = Try(Class.forName(Constants.CONF.getConfig("app.checker").getString("flink")))

  override def installed(): Boolean = {
    try {
      if (name != "flink") {
        _info = name
        Class.forName(Constants.CONF.getConfig(s"app.$model.analyzer").getString(name))
      }
      true
    } catch {
      case e: NoClassDefFoundError =>
        e.printStackTrace()
        _info = model.toString + "." + name
        false
      case e: ClassNotFoundException =>
        e.printStackTrace()
        false
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }

  override def uri: String = cluster

  override def info: String = _info
}

trait ElasticSearchMate extends DriverMate {
  val cluster: String
  val hostPorts: Array[(String, Int)]
}

trait JDBCMate extends DriverMate {
  val protocol: String
  val driver: String
  val host: String
  val port: Int
  val schema: String
  val table: String
  val properties: Option[Properties]
}

trait RabbitMQMate extends DriverMate {
  val host: String
  val port: Option[Int]
  val virtualHost: Option[String]
  val queue: String
  val durable: Boolean
  val username: Option[String]
  val password: Option[String]

}

trait SFTPMate extends DriverMate {
  val host: String
  val port: Option[Int]
  val properties: Option[Properties]

}

trait KafkaMate extends DriverMate with Authorization {
  val topic: Option[String]
  val hostPorts: String
  val properties: Option[Properties]

}

trait HDFSMate extends DriverMate with Authorization {
  // val port: Int
  val nameService: String
  val nameNodes: String
  val host: String
}

trait FTPMate extends DriverMate {
  val host: String
  val port: Option[Int]
  val properties: Option[Properties]

}

trait NetMate extends DriverMate {
  val host: Option[String]
  val port: Int
  val protocol: Option[Protocol]

  def isClient: Boolean

}

trait SyslogMate extends DriverMate {
  val host: String
  val port: Int
  val protocol: Option[String]

}

trait FileMate extends DriverMate {
  val path: String
}

trait PrometheusMate extends DriverMate {
  val host: String
  val port: Int
  val properties: Option[Properties]
}

/*
* host: String, port: Int, properties: Option[Properties]
* */
