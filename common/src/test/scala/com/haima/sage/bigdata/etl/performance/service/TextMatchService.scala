package com.haima.sage.bigdata.etl.performance.service

import java.util.regex.{Matcher, Pattern}

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  *
  * @param field   数据字段
  * @param configs 文本查找配置
  * @param attrs   文本替换配置
  * @param cleans  文本清理配置
  */
class TextMatchService(val field: String,
                       val configs: Seq[TextConfig],
                       val attrs: Seq[XmlToTextConfig] = Nil,
                       val cleans: Seq[String] = Nil,
                       val trim: Boolean = true) {
  private val separator = "\n"
  private val indices: Seq[TextProcess] = Try {
    configs.map(_.service(separator))
  }.getOrElse(Nil)

  private val pattern = Pattern.compile(separator)
  private val needXmlToText = attrs != null && attrs.nonEmpty
  private val xmlToTextService = if (needXmlToText) {
    new XmlToTextService(field, attrs, separator)
  } else {
    null
  }

  def process(event: RichMap): RichMap = Try {
    val map = new mutable.HashMap[String, Any]()
    map ++= event
    val dst = process(map)
    RichMap(dst.toMap)
  }.getOrElse(event)

  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    if (indices.isEmpty) {
      return event
    }
    var value: String = event.get(field) match {
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    if (needXmlToText) {
      value = xmlToTextService.process(value)
    }
    if (cleans.nonEmpty) {
      cleans.foreach(clean => {
        value = value.replace(clean, "")
      })
    }
    val lines: Seq[String] = split(value)
    var dst = new mutable.HashMap[String, Any]()
    for (item <- indices) {
      Try {
        val res = item.process(lines)
        if (res != null) {
          dst += (item.config.index -> res)
        }
      }
    }
    event ++ dst
  }.getOrElse(event)

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
    } else if (index == 0) {
      append(res, raw)
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

/**
  *
  * @param anchor     锚点, 正则
  * @param offset     偏移, 0 自身, >0 向后, <0 向前
  * @param matchType  正则匹配类型：  true 全部匹配（默认），false 查找
  * @param firstGroup offset=0时, 是否返回第一个匹配的项目
  */
case class TextFindConfig(anchor: String,
                          offset: Int = 0,
                          matchType: Boolean = true,
                          firstGroup: Boolean = true
                         )

/**
  * 根据锚点和位移 查找数据
  *
  * @param config 配置数据
  */
class TextFindProcess(val config: TextFindConfig) {
  private val regex = Pattern.compile(config.anchor)

  /**
    * 向后查找最近锚点
    *
    * @param lines           行数据
    * @param startLineNumber 行号
    * @param stopLineNumber  -1不限制
    * @return (Int pattern行号, Int offset行号)未找到，返回 null
    */
  def anchorLineNumberAfter(lines: Seq[String], startLineNumber: Int, stopLineNumber: Int = -1): (Int, Int) = {
    anchorLineNumber(lines, startLineNumber, stopLineNumber, after = true)
  }

  /**
    * 向前/后查找最近锚点
    *
    * @param lines           行数据
    * @param startLineNumber 行号
    * @param stopLineNumber  -1不限制
    * @return (Int pattern行号, Int offset行号)未找到，返回 null
    */
  def anchorLineNumberBefore(lines: Seq[String], startLineNumber: Int, stopLineNumber: Int = -1): (Int, Int) = {
    anchorLineNumber(lines, startLineNumber, stopLineNumber, after = false)
  }

  /**
    * 向前查找最近锚点
    *
    * @param lines           行数据
    * @param startLineNumber 行号
    * @return (Int pattern行号, Int offset行号)未找到，返回 null
    */
  def anchorLineNumber(lines: Seq[String], startLineNumber: Int, stopLineNumber: Int, after: Boolean): (Int, Int) = {
    val range = if (after) {
      val limit = if (stopLineNumber < 0) lines.length else 0
      startLineNumber until limit
    } else {
      val limit = if (stopLineNumber < 0) 0 else stopLineNumber
      (limit until startLineNumber).reverse
    }
    for (i <- range) {
      val line = lines(i)
      val matcher = regex.matcher(line)
      if (isMatch(matcher)) {
        return (i, i + config.offset)
      }
    }
    null
  }

  def isMatch(matcher: Matcher): Boolean = {
    if (config.matchType) {
      matcher.matches()
    } else {
      matcher.find()
    }
  }

  def doMatch(lines: Seq[String], line: String, lineNumber: Int): (Boolean, String) = {
    val matcher = regex.matcher(line)
    if (isMatch(matcher)) {
      if (config.offset == 0) {
        val value = if (config.firstGroup) {
          matcher.group(1)
        } else {
          line
        }
        return (true, value)
      } else {
        val dstIndex = lineNumber + config.offset
        if (dstIndex < -1 || dstIndex > lines.size) {
          return (true, null)
        } else {
          return (true, lines(dstIndex))
        }
      }
    }
    (false, null)
  }

  /**
    * @param lines 行数据
    * @return 未找到，返回null
    */
  def process(lines: Seq[String], startLineNumber: Int): (String, Int) = {
    for (i <- startLineNumber until lines.length) {
      val line = lines(i)
      val res = doMatch(lines, line, i)
      if (res._1) {
        return (res._2, i)
      }
    }
    null
  }

}

object TextFindType extends Enumeration {
  // 单锚点
  val Single: TextFindType.Value = Value("s")
  // 两锚点之间
  val Between: TextFindType.Value = Value("b")
  val Up: TextFindType.Value = Value("u")
  val Down: TextFindType.Value = Value("d")

  def checkExists(day: String): Boolean = this.values.exists(_.toString == day) //检测是否存在此枚举值
  def showAll(): Unit = this.values.foreach(println) // 打印所有的枚举值
}


/**
  *
  * @param index    字段名称
  * @param findType 查找方式
  * @param trim     清除空白
  */
abstract class TextConfig(val index: String,
                          val findType: TextFindType.Value,
                          val trim: Boolean = true,
                          val findTimes: Int = 10) {
  def service(separator: String): TextProcess
}

case class TextSingleConfig(override val index: String,
                            anchor: TextFindConfig,
                            pattern: Option[String] = None,
                            override val trim: Boolean = true,
                            override val findTimes: Int = 10
                           )
  extends TextConfig(index, TextFindType.Single, trim, findTimes) {
  override def service(separator: String): TextProcess = Try {
    TextSingleProcess(this, separator)
  }.getOrElse(null)
}

case class TextBetweenConfig(override val index: String,
                             firstAnchor: TextFindConfig,
                             secondAnchor: TextFindConfig,
                             override val trim: Boolean = true,
                             override val findTimes: Int = 10,
                             singleNode: Boolean = true
                            )
  extends TextConfig(index, TextFindType.Single, trim, findTimes) {
  override def service(separator: String): TextProcess = Try {
    if (singleNode) {
      new TextBetweenSingleProcess(this, separator)
    } else {
      new TextBetweenMultiProcess(this, separator)
    }
  }.getOrElse(null)
}

case class TextUpConfig(override val index: String,
                        firstAnchor: TextFindConfig,
                        secondAnchor: TextFindConfig,
                        override val trim: Boolean = true,
                        override val findTimes: Int = 10
                       )
  extends TextConfig(index, TextFindType.Single, trim, findTimes) {
  override def service(separator: String): TextProcess = Try {
    TextUpProcess(this, separator)
  }.getOrElse(null)
}

case class TextDownConfig(override val index: String,
                          firstAnchor: TextFindConfig,
                          secondAnchor: TextFindConfig,
                          override val trim: Boolean = true,
                          override val findTimes: Int = 10
                         )
  extends TextConfig(index, TextFindType.Single, trim, findTimes) {
  override def service(separator: String): TextProcess = Try {
    DownTextProcess(this, separator)
  }.getOrElse(null)
}

abstract class TextProcess(val config: TextConfig, val separator: String) {
  protected val findTimes: Int = if (config.findTimes > 0) config.findTimes else 10

  def process(lines: Seq[String]): Any
}

case class TextSingleProcess(override val config: TextSingleConfig,
                             override val separator: String)
  extends TextProcess(config, separator) {
  private val textFindProcess = new TextFindProcess(config.anchor)
  private val pattern = Try {
    config.pattern match {
      case Some(x) =>
        Pattern.compile(x)
      case None =>
        null
    }
  }.getOrElse(null)

  def process(lines: Seq[String]): String = {
    var start = 0
    for (_ <- 0 to findTimes) {
      val res = find(lines, start)
      if (res == null) {
        return null
      }
      if (res._2 == -1) {
        return res._1
      }
      if (res._1 != null) {
        if (pattern == null) {
          return res._1
        }
        if (pattern.matcher(res._1).matches()) {
          return res._1
        }
      }
      start = res._2 + 1
    }
    null
  }

  def find(lines: Seq[String], start: Int): (String, Int) = {
    textFindProcess.process(lines, start)
  }
}

/**
  * 最近的两个锚点
  *
  * @param config 锚点配置
  */
class TextBetweenSingleProcess(override val config: TextBetweenConfig,
                               override val separator: String)
  extends TextProcess(config, separator) {
  private val firstTextFindProcess = new TextFindProcess(config.firstAnchor)
  private val secondTextFindProcess = new TextFindProcess(config.secondAnchor)

  def process(lines: Seq[String]): Any = {
    var start = 0
    for (_ <- 0 to findTimes) {
      val res = find(lines, start)
      if (res == null) {
        return null
      }
      if (res._2 == -1) {
        return res._1
      }
      if (res._1 != null) {
        return res._1
      }
      start = res._2 + 1
    }
    null
  }

  /**
    * 查找两个锚点之间的数据，必须：起始锚点的位置>=结束锚点的位置
    *
    * @param lines 行数据
    * @param start 起始点
    * @return (String 结果, Int pattern位置,  Int offset位置)
    */
  def find(lines: Seq[String], start: Int): (String, Int, Int) = {
    val line1 = firstTextFindProcess.anchorLineNumberAfter(lines, start)
    if (line1 == null) {
      return null
    }
    val line2 = secondTextFindProcess.anchorLineNumberAfter(lines, line1._2)
    if (line2 == null) {
      return null
    }
    if (line1._1 > line2._1) { // 必须: 开始锚点的位置 <= 结束锚点的位置 (混乱数据)
      return (null, line2._1, line2._2)
    }
    if (line1._2 > line2._2) { // 必须：开始锚点offset <= 结束锚点offset (空数据)
      return (null, line2._1, line2._2)
    }
    if (line1._2 == line2._2) { // 单行：开始锚点offset = 结束锚点offset
      // 最后一个锚点
      return (between(lines, line1._2, line2._2), line2._2, line2._1)
    }
    val line3 = firstTextFindProcess.anchorLineNumberBefore(lines, line2._1, line1._1)
    val from = if (line3 != null && line3._2 > line1._2) {
      line3._2
    } else {
      line1._2
    }
    (between(lines, from, line2._2), line2._1, line2._2)
  }

  /**
    *
    * @param lines 行数据集
    * @param start 开始, 包括本行
    * @param stop  结束, 包括本行
    * @return
    */
  private def between(lines: Seq[String], start: Int, stop: Int): String = {
    (start to stop).map(lines(_)).mkString(separator)
  }
}

/**
  * 最近的两个锚点得到一条数据
  * 得到多条数据
  *
  * @param config 锚点配置
  */
class TextBetweenMultiProcess(override val config: TextBetweenConfig,
                              override val separator: String)
  extends TextBetweenSingleProcess(config, separator) {
  override def process(lines: Seq[String]): Any = {
    var list = new ListBuffer[String]()

    var start = 0
    var flag = true
    while (flag) {
      val res = find(lines, start)
      if (res == null) {
        return list
      }
      if (res._2 != -1 && res._1 != null) {
        list += res._1
      }
      start = res._2 + 1
      flag = if (start >= lines.length) {
        false
      } else {
        true
      }
    }
    list
  }
}

case class TextUpProcess(override val config: TextUpConfig,
                         override val separator: String)
  extends TextProcess(config, separator) {
  private val firstTextFindProcess = new TextFindProcess(config.firstAnchor)
  private val secondTextFindProcess = new TextFindProcess(config.secondAnchor)

  def process(lines: Seq[String]): String = {
    val line1 = firstTextFindProcess.anchorLineNumberAfter(lines, 0)
    if (line1 == null) {
      return null
    }
    val base = line1._2
    for (i <- (0 to base).reverse) {
      val line = lines(i)
      val res = secondTextFindProcess.doMatch(lines, line, i)
      if (res._1) {
        return res._2
      }
    }
    null
  }
}

case class DownTextProcess(override val config: TextDownConfig,
                           override val separator: String)
  extends TextProcess(config, separator) {
  private val firstTextFindProcess = new TextFindProcess(config.firstAnchor)
  private val secondTextFindProcess = new TextFindProcess(config.secondAnchor)

  def process(lines: Seq[String]): String = {
    val line1 = firstTextFindProcess.anchorLineNumberAfter(lines, 0)
    if (line1 == null) {
      return null
    }
    val start = line1._2
    val stop = lines.size - 1
    for (i <- start to stop) {
      val line = lines(i)
      val res = secondTextFindProcess.doMatch(lines, line, i)
      if (res._1) {
        return res._2
      }
    }
    null
  }
}