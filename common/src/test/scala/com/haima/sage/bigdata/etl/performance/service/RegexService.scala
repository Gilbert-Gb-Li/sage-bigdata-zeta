package com.haima.sage.bigdata.etl.performance.service

import java.util.regex.{Matcher, Pattern}

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.Try

class RegexService(val configs: RegexConfig*) {
  private val services = RegexServiceFactory.factory(configs)

  def valid(): Boolean = services.nonEmpty

  def process(event: RichMap): RichMap = Try {
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


class SingleRegexItemService(override val config: RegexConfig)
  extends RegexItemService(config) {
  private val findConfig = Try(config.findConfigs.head).getOrElse(null)
  private val findService = new RegexFindService(findConfig)

  override def valid(): Boolean = {
    if (config == null || findService == null) {
      return false
    }
    if (!findService.valid()) {
      return false
    }
    true
  }

  override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    if (findService == null) {
      return event
    }
    val value: String = event.get(config.field) match {
      case Some(x: Any) =>
        x.toString
      case None => null
    }
    if (value == null) {
      return event
    }
    findService.process(value, event)._2
  }.getOrElse(event)
}

object RegexLogicType extends Enumeration {
  val AND: RegexLogicType.Value = Value(1)
  val OR: RegexLogicType.Value = Value(2)
}


abstract class RegexItemService(val config: RegexConfig) {
  def valid(): Boolean

  def process(event: RichMap): RichMap = Try {
    val map = new mutable.HashMap[String, Any]()
    map ++= event
    val dst = process(map)
    RichMap(dst.toMap)
  }.getOrElse(event)

  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any]
}

object RegexServiceFactory {
  def factory(configs: Seq[RegexConfig]): Seq[RegexItemService] = Try {
    configs.map(config => {
      newRegexItemService(config)
    }).filter(_.valid())
  }.getOrElse(Nil)

  def newRegexItemService(config: RegexConfig): RegexItemService = Try {
    if (config.findConfigs.size == 1) {
      return new SingleRegexItemService(config)
    }
    if (config.logic == RegexLogicType.AND) {
      return new AndRegexItemService(config)
    }
    new OrRegexItemService(config)
  }.getOrElse(null)
}

case class RegexConfig(field: String,
                       logic: RegexLogicType.Value,
                       findConfigs: Seq[RegexFindConfig]) {
  def this(field: String,
           pattern: String,
           fields: Map[Int, String],
           matchType: Boolean = true,
           trim: Boolean = true) {
    this(field, RegexLogicType.AND,
      Seq(RegexFindConfig(pattern, fields, matchType, trim)))
  }
}


class RegexFindService(val config: RegexFindConfig) {
  private val pattern = Try(Pattern.compile(config.pattern)).getOrElse(null)

  def valid(): Boolean = {
    if (pattern == null) {
      return false
    }
    true
  }

  /**
    *
    * @param value 字段值
    * @param event 匹配结果放入map
    * @return (是否匹配成功， )
    */
  def process(value: String, event: mutable.Map[String, Any]): (Boolean, mutable.Map[String, Any]) = {
    if (pattern == null) {
      return null
    }
    val matcher = pattern.matcher(value)
    val matchRes: Boolean = matchResult(config.matchType, matcher)

    if (matchRes) {
      config.fields.foreach(entry => {
        val index = entry._1
        val field = entry._2
        val text = matcher.group(index) match {
          case null =>
            null
          case x =>
            if (config.trim) {
              x.trim
            } else {
              x
            }
        }
        if (text != null && text.length > 0) {
          event += (field -> text)
        }
      })
      (true, event)
    } else {
      (false, event)
    }
  }

  private def matchResult(matchType: Boolean, matcher: Matcher): Boolean = {
    if (matchType) {
      matcher.find()
    } else {
      matcher.matches()
    }
  }
}


case class RegexFindConfig(pattern: String,
                           fields: Map[Int, String],
                           matchType: Boolean = true,
                           trim: Boolean = true)

class OrRegexItemService(override val config: RegexConfig)
  extends RegexItemService(config) {
  private val findServices = Try {
    config.findConfigs.map(findConfig => {
      new RegexFindService(findConfig)
    }).filter(_.valid())
  }.getOrElse(Nil)

  override def valid(): Boolean = {
    if (findServices == null || findServices.isEmpty) {
      return false
    }
    true
  }

  override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(x: Any) =>
        x.toString
      case None => null
    }
    if (value == null) {
      return event
    }
    for (findService <- findServices) Try {
      val res = findService.process(value, event)
      if (res != null && res._1) { // 正则匹配了，返回查找到的结果
        return event
      }
    }
    // 未匹配到，原始返回
    event
  }.getOrElse(event)
}


class AndRegexItemService(override val config: RegexConfig)
  extends RegexItemService(config) {
  private val findServices = Try {
    config.findConfigs.map(findConfig => {
      new RegexFindService(findConfig)
    }).filter(_.valid())
  }.getOrElse(Nil)

  override def valid(): Boolean = {
    if (findServices == null || findServices.isEmpty) {
      return false
    }
    true
  }

  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(x: Any) =>
        x.toString
      case None => null
    }
    if (value == null) {
      return event
    }
    for (findService <- findServices) Try {
      findService.process(value, event)
    }
    event
  }.getOrElse(event)
}
