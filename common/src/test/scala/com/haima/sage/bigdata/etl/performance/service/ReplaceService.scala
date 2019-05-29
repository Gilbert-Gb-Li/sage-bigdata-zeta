package com.haima.sage.bigdata.etl.performance.service

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.Try


case class ReplaceService(val configs: ReplaceConfig*) {
  private val services = ReplaceServiceFactory.factory(configs)

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

object ReplaceServiceFactory {
  def factory(configs: Seq[ReplaceConfig]): Seq[ReplaceItemService] = Try {
    configs.map(new ReplaceItemService(_))
  }.getOrElse(Nil)
}

class ReplaceItemService(val config: ReplaceConfig) {
  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }

    event += (config.field -> value.replace(config.from, config.to))
  }.getOrElse(event)

  def process(event: RichMap): RichMap = Try {
    val value: String = event.get(config.field) match {
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    event + (config.field -> value.replace(config.from, config.to))
  }.getOrElse(event)
}

case class ReplaceConfig(field: String, from: String, to: String = "")