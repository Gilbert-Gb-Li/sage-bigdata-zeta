package com.haima.sage.bigdata.analyzer.aggregation.model

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.commons.collections4.ListUtils

import scala.collection.JavaConversions._


/**
  * Created by wxn on 2017/10/31.
  */
case class Pattern(pattern: List[PatternElement], total: Int = 1) {
  @JsonIgnore
  lazy val texts: List[PatternText] = pattern.filter(_.isInstanceOf[PatternText]).map(_.asInstanceOf[PatternText])
  private lazy val terms: List[String] = texts.flatMap(d => Terms(d.text).terms)
  @JsonIgnore
  lazy val length: Int = texts.size

  @JsonIgnore
  def params: List[PatternParameter] = {
    pattern.filter(_.isInstanceOf[PatternParameter]).map(_.asInstanceOf[PatternParameter])
  }

  def toParams: String = {
    params.filter(_ != null).mkString(",")
  }

  def toPatterns: String = {

    texts.filter(_ != null).mkString(",")
  }

  def toKey: String = {
    pattern.map {
      case text: PatternText =>
        text.text.trim
      case _: PatternParameter =>
        "*"
      case _ =>
        null
    }.filter(_ != null).mkString("")
  }

  def getLcs: String = {
    pattern.filter(_.isInstanceOf[PatternText]).mkString(",")
  }

  override def toString: String = {
    pattern.map {
      case text: PatternText =>
        val patternText = text.text
        s"""{"text":"$patternText"}"""
      case parameter: PatternParameter =>
        val patternParam = parameter.param
        s"""{"param":"$patternParam"}"""
      case _ =>
        null
    }.filter(_ != null).mkString("[", ",", "]")
  }

  def isMatch(data: Terms): Boolean = {
    if (terms.isEmpty) {
      false
    } else {
      val lcs: List[String] = ListUtils.longestCommonSubsequence(terms, data.terms).toList
      lcs.nonEmpty && lcs.size == terms.size

    }
  }
}

object Pattern {
  def apply(pattern: String): Pattern = {

    val ps = pattern.substring(3, pattern.length - 3).split("\"\\},\\{\"")
    val patterns = ps.map(d => {
      val ds = d.split("\":\"")
      if (ds(0) == "param") {
        PatternParameter(ds(1))
      } else {
        PatternText(ds(1))
      }
    }).toList
    Pattern(patterns, 0)


  }

}
