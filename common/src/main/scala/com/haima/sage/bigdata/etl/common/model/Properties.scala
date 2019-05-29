package com.haima.sage.bigdata.etl.common.model

import java.io.IOException
import java.util.{Properties => Props}

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by zhhuiyan on 15/4/9.
  */
object Properties {

  class Properties

  private val logger: Logger = LoggerFactory.getLogger(classOf[Properties])
  val loader: ClassLoader = classOf[Properties].getClassLoader

  def apply(path: String): WithProperties = {
    val props = new Props()
    try {
      props.load(loader.getResourceAsStream(path))
    } catch {
      case e: IOException =>
        logger.trace("error:{}", e);
    }
    new WithProperties {
      override val properties: Option[Props] = Some(props)
    }
  }

  def apply(value: Map[String, String] = Map()): WithProperties {
    val properties: Option[Props]
  } = {
    val props = new Props()

    value.foreach { tuple =>
      val v: String = tuple._2
      props.setProperty(tuple._1, v)
    }
    new WithProperties {
      override val properties: Option[Props] = Some(props)
    }
  }

  def unapply(properties: WithProperties): Map[String, Any] = properties.toMap

  def unapply(properties: Props): Map[String, Any] = properties.keySet().toArray.map {
    key: AnyRef =>
      (key.toString, properties.get(key))
  }.toMap
}

trait WithProperties {
  def properties: Option[Props]

  def get(key: String, default: String): String = {
    properties match {
      case Some(props) if props != null =>
        props.getProperty(key, default)
      case _ =>
        default

    }
  }

  def get(key: String): Option[String] = {
    properties match {
      case Some(props) if props != null =>
        props.getProperty(key) match {
          case value: String if value != null && value.trim != "" =>
            Some(value)
          case _ =>
            None
        }
      case _ =>
        None

    }
  }

  def toMap: Map[String, Any] =
    properties match {
      case None =>
        Map()
      case Some(props) =>
        props.keySet().toArray.map {
          key: AnyRef =>
            (key.toString, props.get(key))
        }.toMap
    }


}
