package com.haima.sage.bigdata.etl.performance.service

import java.math.BigDecimal

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class UnknownNumberItemService(override val config: NumberItemConfig)
  extends NumberItemService(config) {


  override def valid(): Boolean = {
    false
  }

  override def process(value: String): Number = {
    null
  }
}

class NumberService(val configs: NumberItemConfig*) {
  private val services = NumberServiceFactory.factory(configs)

  def valid(): Boolean = services.nonEmpty

  def process(event: RichMap): RichMap = Try {
    if (services.size == 1) {
      return services.head.process(event)
    }
    val map = new mutable.HashMap[String, Any]()
    map ++= event
    val dst = process(map)
    RichMap(dst.toMap)
  }.getOrElse(event)

  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    services.foreach(service => {
      service.process(event)
    })
    event
  }.getOrElse(event)
}

abstract class NumberItemService(val config: NumberItemConfig) {
  def valid(): Boolean

  def process(event: RichMap): RichMap = Try {
    val map: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    map ++= event
    val value: String = event.get(config.field) match {
      case Some(x: Any) =>
        x.toString.trim
      case None => null
    }
    val dstValue = process(value)
    map += (config.field -> dstValue)
    RichMap(map.toMap)
  }.getOrElse(event - config.field)

  def process(map: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = map.get(config.field) match {
      case Some(x: Any) =>
        x.toString.trim
      case None => null
    }
    val dstValue = process(value)
    map += (config.field -> dstValue)
  } match {
    case Success(value) =>
      value
    case Failure(exception) =>
      map -= config.field
  }

  def process(value: String): Number
}

object NumberServiceFactory {
  def factory(configs: Seq[NumberItemConfig]): Seq[NumberItemService] = Try {
    configs.map(config => {
      newNumberItemService(config)
    }).filter(_.valid())
  }.getOrElse(Nil)

  def newNumberItemService(config: NumberItemConfig): NumberItemService = Try {
    config.cleanType match {
      case "english-w-upper" | "W" =>
        new EndOnyNumberItemService(config, "W", 10000)
      case "english-w-lower" | "w" =>
        new EndOnyNumberItemService(config, "w", 10000)
      case "english-w-upper" | "K" =>
        new EndOnyNumberItemService(config, "K", 1000)
      case "english-w-lower" | "k" =>
        new EndOnyNumberItemService(config, "k", 1000)
      case "chinese-shi" | "shi" =>
        new EndOnyNumberItemService(config, "十", 10)
      case "chinese-bai" | "bai" =>
        new EndOnyNumberItemService(config, "百", 100)
      case "chinese-qian" | "qian" =>
        new EndOnyNumberItemService(config, "千", 1000)
      case "chinese-wan" | "wan" =>
        new EndOnyNumberItemService(config, "万", 10000)
      case "chinese-shiwan" | "shiwan" =>
        new EndOnyNumberItemService(config, "十万", 100000)
      case "chinese-baiwan" | "baiwan" =>
        new EndOnyNumberItemService(config, "百万", 1000000)
      case "chinese-qianwan" | "qianwan" =>
        new EndOnyNumberItemService(config, "千万", 10000000)
      case "chinese-yi" | "yi" =>
        new EndOnyNumberItemService(config, "亿", 100000000)
      case "none-or-single" | "either" =>
        new EitherNumberItemService(config)
      case "none-or-single" | "none" =>
        new NoneNumberItemService(config)
      case _ =>
        null
    }
  }.getOrElse(null)
}

case class NumberItemConfig(field: String,
                            cleanType: String,
                            needReplace: Boolean = true,
                            dotNumber: Boolean = false)

class EitherNumberItemService(override val config: NumberItemConfig)
  extends NumberItemService(config) {
  private val kService = {
    NumberServiceFactory.newNumberItemService(
      NumberItemConfig(config.field, "k", needReplace = false, config.dotNumber))
  }
  private val wanService = {
    NumberServiceFactory.newNumberItemService(
      NumberItemConfig(config.field, "w", needReplace = false, config.dotNumber))
  }
  private val yiService = {
    NumberServiceFactory.newNumberItemService(
      NumberItemConfig(config.field, "yi", needReplace = false, config.dotNumber))
  }
  private val noneService = {
    NumberServiceFactory.newNumberItemService(
      NumberItemConfig(config.field, "none", needReplace = false, config.dotNumber))
  }

  override def valid(): Boolean = {
    true
  }

  override def process(raw: String): Number = {
    val lastChar: Char = raw.charAt(raw.length - 1)
    if (lastChar == 'w' || lastChar == 'W' || lastChar == '万') {
      wanService.process(raw.substring(0, raw.length - 1).trim)
    } else if (lastChar == '亿') {
      yiService.process(raw.substring(0, raw.length - 1).trim)
    } else {
      noneService.process(raw)
    }
  }
}


class NoneNumberItemService(override val config: NumberItemConfig)
  extends NumberItemService(config) {


  override def valid(): Boolean = {
    true
  }

  override def process(raw: String): Number = {
    val dst: BigDecimal = new BigDecimal(raw)
    if (config.dotNumber) {
      dst.doubleValue()
    } else {
      dst.longValue()
    }
  }
}

class EndOnyNumberItemService(override val config: NumberItemConfig,
                              val from: String,
                              val unit: Int)
  extends NumberItemService(config) {

  override def valid(): Boolean = {
    true
  }

  override def process(raw: String): Number = {
    val value: String = if (config.needReplace) {
      raw.replace(from, "").trim
    } else {
      raw.trim
    }
    val dst: BigDecimal = new BigDecimal(value).multiply(new BigDecimal(unit))
    if (config.dotNumber) {
      dst.doubleValue()
    } else {
      dst.longValue()
    }
  }
}
