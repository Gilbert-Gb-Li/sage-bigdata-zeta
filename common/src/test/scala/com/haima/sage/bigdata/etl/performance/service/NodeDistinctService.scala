package com.haima.sage.bigdata.etl.performance.service

import java.util.regex.Pattern

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

class NodeDistinctService(configs: Seq[DistinctConfig]) {
  private val services = NodeDistinctFactory.factory(configs)

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

object NodeDistinctFactory {
  def factory(configs: Seq[DistinctConfig]): Seq[NodeDistinctProcess] = Try {
    configs.map(config => {
      config.newService()
    }).filter(_.valid())
  }.getOrElse(Nil)

}

case class DistinctConfig(field: String,
                          separator: String = "\n",
                          ignores: Seq[String] = Nil,
                          trim: Boolean = true
                         ) {
  def newService(): NodeDistinctProcess = {
    new NodeDistinctProcess(this)
  }
}

class NodeDistinctProcess(config: DistinctConfig) {
  private val trim = config.trim
  private val field = Try {
    config.field.trim()
  }.getOrElse("")
  private val pattern = Try {
    Pattern.compile(config.separator)
  }.getOrElse(null)
  private val ignoresPattern = Try {
    config.ignores match {
      case null | Nil =>
        Nil
      case _ =>
        config.ignores.map(ignore => {
          Try {
            Pattern.compile(ignore)
          }.getOrElse(null)
        }).filter(_ != null)
    }
  }

  def valid(): Boolean = {
    if (field.isEmpty) {
      return false
    }
    if (pattern == null) {
      return false
    }
    true
  }

  def process(event: RichMap): RichMap = Try {
    val map = new mutable.HashMap[String, Any]()
    map ++= event
    val dst = process(map)
    RichMap(dst.toMap)
  }.getOrElse(event)

  def isIgnoreDistinct(line: String): Boolean = {
    true
  }

  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = {
    val raw = event.get(field) match {
      case Some(x) =>
        if (config.trim) {
          x.toString.trim
        } else {
          x.toString
        }
      case None =>
        null
    }
    if (raw == null) {
      return event
    }
    val lines = split(raw)
    val dst = new ListBuffer[String]
    lines.foreach(line => {
      if (isIgnoreDistinct(line)) {

      }
    })
    // TODO
    event
  }

  /**
    * 忽略 空白行
    *
    * @param raw 欲拆分的文本
    * @return
    */
  def split(raw: String): Seq[String] = {
    val res = new ListBuffer[String]
    val m = pattern.matcher(raw)
    var index = 0
    var key = 1
    while (m.find()) {
      val value = raw.substring(index, m.start)
      append(res, value)
      index = m.end
      key += 1
    }
    if (index > 0) {
      if (index != raw.length - 1) {
        val value = raw.substring(index, raw.length)
        append(res, value)
      }
    }
    res
  }


  /**
    * 忽略空白行
    *
    * @param res   结果集合
    * @param value 单行数据
    * @return
    */
  def append(res: ListBuffer[String], value: String): ListBuffer[String] = {
    if (value == null) {
      return res
    }
    var dst = value
    if (trim) {
      dst = value.trim
    }
    if (dst.isEmpty) {
      return res
    }
    res += dst
  }
}