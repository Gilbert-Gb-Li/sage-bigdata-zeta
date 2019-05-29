package com.haima.sage.bigdata.etl.performance.service

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.Try

case class ContainsService(val configs: ContainsConfig*) {
  private val services = ContainsServiceFactory.factory(configs)

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
    services.foreach(service => {
      service.process(event)
    })
    event
  }.getOrElse(event)
}

object ContainsServiceFactory {
  def factory(configs: Seq[ContainsConfig]): Seq[ContainsItemService] = Try {
    configs.map(new ContainsItemService(_))
  }.getOrElse(Nil)
}

class ContainsItemService(val config: ContainsConfig) {
  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    if (value.contains(config.containsValue)) {
      event += (config.yesSet.field -> config.yesSet.value)
    } else {
      if (config.noSet != null) {
        event += (config.noSet.field -> config.noSet.value)
      }
    }
    event
  }.getOrElse(event)

  def process(event: RichMap): RichMap = Try {
    val value: String = event.get(config.field) match {
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    var res = event
    if (value.contains(config.containsValue)) {
      res += (config.yesSet.field -> config.yesSet.value)
    } else {
      if (config.noSet != null) {
        res += (config.noSet.field -> config.noSet.value)
      }
    }
    res
  }.getOrElse(event)
}

case class ContainsConfig(field: String,
                          containsValue: String,
                          yesSet: ContainsSetConfig,
                          noSet: ContainsSetConfig = null)


case class ContainsSetConfig(field: String, value: Any)