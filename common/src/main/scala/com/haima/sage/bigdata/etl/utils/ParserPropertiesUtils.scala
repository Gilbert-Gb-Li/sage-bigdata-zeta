package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.model.ParserProperties

object ParserPropertiesUtils {

  def merge(first: List[ParserProperties], second: List[ParserProperties]): List[ParserProperties] = {
    second.filterNot(props =>
      if (first == null || first.isEmpty) {
        false
      } else {
        first.exists(prop => prop.key == props.key)
      }
    )
    (first, second) match {
      case (null, s) =>
        s
      case (f, null) =>
        f
      case (_, Nil) =>
        first
      case (Nil, _) =>
        second
      case (_, _) =>
        val keys = first.map(_.key)
        second.filterNot(d => keys.contains(d.key)) ::: first
    }
  }

  def mergeWithType: (List[ParserProperties], List[ParserProperties]) => List[ParserProperties] = {
    case (_new, Nil) =>
      _new
    case (Nil, old) =>
      old
    case (_new, old) =>
      (_new ++ old.filter(p=> ! _new.exists(_.key==p.key))).distinct

  }
}
