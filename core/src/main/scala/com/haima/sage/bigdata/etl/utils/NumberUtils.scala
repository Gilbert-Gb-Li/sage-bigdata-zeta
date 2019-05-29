package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.util.Try

object NumberUtils {
  def textToDouble(attrs: String*)(implicit event: RichMap): RichMap = {
    var dst = event
    attrs.foreach(attr => Try {
      processOne(attr)(event) match {
        case Some(x) =>
          dst += attr -> x
        case None =>
          dst += attr -> null
      }
    })
    dst
  }

  def textToFloat(attrs: String*)(implicit event: RichMap): RichMap = {
    var dst = event
    attrs.foreach(attr => Try {
      processOne(attr)(event) match {
        case Some(x) =>
          dst += attr -> x.toFloat
        case None =>
          dst += attr -> null
      }
    })
    dst
  }

  def textToLong(attrs: String*)(implicit event: RichMap): RichMap = {
    var dst = event
    attrs.foreach(attr => Try {
      processOne(attr)(event) match {
        case Some(x) =>
          dst += attr -> x.toLong
        case None =>
          dst += attr -> null
      }
    })
    dst
  }

  def textToInt(attrs: String*)(implicit event: RichMap): RichMap = {
    var dst = event
    attrs.foreach(attr => Try {
      processOne(attr)(event) match {
        case Some(x) =>
          dst += attr -> x.toInt
        case None =>
          dst += attr -> null
      }
    })
    dst
  }

  private def processOne(attr: String)(event: RichMap): Option[Double] = {
    val value = event.getOrElse(attr, null)
    if (value == null) {
      return None
    }
    number(value.toString)
  }

  private def toDouble(str: String): Option[Double] = Try {
    Some(str.toDouble)
  }.getOrElse(None)

  private def number(text: String): Option[Double] = {
    if (text.endsWith("万")) {
      toDouble(text.substring(0, text.length - 1)) match {
        case Some(x) => Some(x * 10000)
        case None =>
          None
      }
    } else if (text.endsWith("w") || text.endsWith("W")) {
      toDouble(text.substring(0, text.length - 1)) match {
        case Some(x) => Some(x * 10000)
        case None =>
          None
      }
    } else {
      None
    }
  }

}
