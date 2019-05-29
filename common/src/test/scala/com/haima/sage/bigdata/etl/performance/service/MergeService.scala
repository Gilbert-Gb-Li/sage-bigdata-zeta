package com.haima.sage.bigdata.etl.performance.service

import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.Try

case class MergeService(configs: MergeConfig*) {
  private val services = MergeServiceFactory.factory(configs)

  def valid(): Boolean = services.nonEmpty

  def process(event: RichMap): RichMap = Try {
    if (services.length == 1) {
      services.head.process(event)
    } else {
      val map = new mutable.HashMap[String, Any]()
      map ++= event
      val dst = process(map)
      event ++ dst
    }

  }.getOrElse(event)

  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    if (services.length == 1) {
      services.head.process(event)
    } else {
      services.foreach(service => {
        service.process(event)
      })
      event
    }
  }.getOrElse(event)
}

object MergeServiceFactory {
  def factory(configs: Seq[MergeConfig]): Seq[MergeItemService] = Try {
    configs.map(new MergeItemService(_))
  }.getOrElse(Nil)
}

case class MergeConfig(dstField: String,
                       fromFields: Seq[String],
                       allowItemEmpty: Boolean = false,
                       separator: String = "",
                       trim: Boolean = true
                      )

case class MergeItemService(config: MergeConfig) {
  val dstField = Try {
    config.dstField.trim
  }.getOrElse(null)
  val fromFields = Try {
    config.fromFields.filter(_ != null).map(_.trim).filter(_.nonEmpty)
  }.getOrElse(Nil)
  val separator = Try {
    config.separator.trim
  }.getOrElse("")
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def valid(): Boolean = {
    if (config == null) {
      return false
    }
    if (config.dstField == null || dstField.isEmpty) {
      return false
    }
    if (config.fromFields == null || config.fromFields.isEmpty || fromFields.isEmpty) {
      return false
    }
    true
  }

  private def getFromValues(event: mutable.Map[String, Any]): Seq[String] = {
    fromFields.indices.map(i => {
      val field = fromFields(i)
      val value = event.getOrElse(field, null)
      if (value == null) {
        ""
      } else {
        val dstValue = format(value)
        if (config.trim) {
          dstValue.trim
        } else {
          dstValue
        }
      }
    })
  }

  private def getFromValues(event: RichMap): Seq[String] = {
    fromFields.indices.map(i => {
      val field = fromFields(i)
      val value = event.getOrElse(field, null)
      if (value == null) {
        ""
      } else {
        val dstValue = format(value)
        if (config.trim) {
          dstValue.trim
        } else {
          dstValue
        }
      }
    })
  }

  def format(value: Any): String = {
    value match {
      case x: Date =>
        sdf.format(x)
      case x: Any =>
        x.toString
    }
  }

  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val values = getFromValues(event)
    if (!config.allowItemEmpty) {
      if (values.exists(_.isEmpty)) {
        return event
      }
    }
    val dstValue = values.mkString(separator)
    event += (dstField -> dstValue)
  }.getOrElse(event)

  def process(event: RichMap): RichMap = Try {
    val values = getFromValues(event)
    if (!config.allowItemEmpty) {
      if (values.exists(_.isEmpty)) {
        return event
      }
    }
    val dstValue = values.mkString(separator)
    event + (dstField -> dstValue)
  }.getOrElse(event)
}