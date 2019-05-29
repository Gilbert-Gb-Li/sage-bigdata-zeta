package com.haima.sage.bigdata.etl.utils

import com.google.code.regexp.{Matcher, Pattern}
import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try


object RegexFindUtils {
  val cache = new mutable.WeakHashMap[String, (Pattern, List[String])]()

  def getPattern(path: String): (Pattern, List[String]) = {
    val compile = cache.getOrElse(path, null)
    if (compile != null) {
      return compile
    }
    val newCompile = Pattern.compile(path)

    val res = (newCompile, asScalaIterator(newCompile.groupNames().iterator()).toList)
    cache.put(path, res)
    res
  }

  /**
    * 全部匹配
    *
    * @param field     字段名称
    * @param patterns  正则列表
    * @param event     键值数据
    * @param matchType 匹配方式：默认true, 查找; false匹配
    * @return
    */
  def regexBoth(field: String, patterns: String*)
               (implicit event: RichMap, matchType: Boolean = true): RichMap = Try {
    if (patterns == null || patterns.isEmpty) {
      return event
    }
    val value: String = event.get(field) match {
      case Some(x: Any) =>
        x.toString
      case None => null
    }
    if (value == null) {
      return event
    }
    var dst = event
    for (pattern <- patterns) Try {
      val res = processOne(pattern, value)(dst, matchType)
      dst = res._2
    }
    dst
  }.getOrElse(event)

  /**
    * 首次匹配成功就结束
    *
    * @param field    字段名称
    * @param patterns 正则列表
    * @param event    键值数据
    * @return
    */
  def regex(field: String, patterns: String*)
           (implicit event: RichMap): RichMap = Try {
    if (patterns == null || patterns.isEmpty) {
      return event
    }
    val value: String = event.get(field) match {
      case Some(x: Any) =>
        x.toString
      case None => null
    }
    if (value == null) {
      return event
    }
    for (pattern <- patterns) Try {
      val res = processOne(pattern, value)(event)
      if (res._1) {
        return res._2
      }
    }
    event
  }.getOrElse(event)

  def processOne(pattern: String, value: String)
                (event: RichMap, matchType: Boolean = true): (Boolean, RichMap) = {
    val tuple: (Pattern, List[String]) = getPattern(pattern)
    val matcher = tuple._1.matcher(value)
    val matchRes: Boolean = matchResult(matchType, matcher)
    if (matchRes) {
      var dst = event
      tuple._2.foreach(name => Try {
        val text = matcher.group(name) match {
          case null =>
            null
          case x =>
            x.trim
        }
        dst = dst + (name -> text)
      })
      (true, dst)
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
