package com.haima.sage.bigdata.etl.plugin.es_6.utils

import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.common.xcontent.{XContent, XContentBuilder}

case class XBuilder(xContent: XContent = JsonXContent.jsonXContent) {


  private final val builder = XContentBuilder.builder(xContent)


  def bytes(map: Map[String, Any]): XContentBuilder = {
    ensureNoSelfReferences(map)
    dealMap(map)
    builder
  }


  private def unknownValue(value: Any): Unit = {
    value match {
      case null =>
        builder.nullValue
      case map: Map[String@unchecked, Any@unchecked] => dealMap(map)
      case value1: Array[Any@unchecked] => dealScalaArray(value1)
      case value1: Iterable[Any@unchecked] => dealScalaIterable(value1)
      case obj => builder.value(obj)
    }
  }


  private def dealScalaIterable(value: Iterable[_]): Unit = {
    if (value != null && value.nonEmpty) {
      builder.startArray
      value.foreach(unknownValue)
      builder.endArray
    }
    else builder.nullValue
  }


  private def dealScalaArray(value: Array[_]): Unit = {
    builder.startArray
    value.foreach(unknownValue)
    builder.endArray
  }


  private def dealMap(map: Map[String, Any]): Unit = {
    if (map != null) {
      builder.startObject

      map.foreach(tuple => {
        builder.field(tuple._1.toString)
        unknownValue(tuple._2)
      })
    } else
      builder.nullValue
  }

  import java.nio.file.Path
  import java.util
  import java.util.Collections

  private[utils] def ensureNoSelfReferences(value: Any): Unit = {
    val it: Iterable[_] = convert(value)

    val identityHashMap: util.Map[Any, java.lang.Boolean] = new util.IdentityHashMap[Any, java.lang.Boolean]

    val ancestors: util.Set[Any] = Collections.newSetFromMap[Any](identityHashMap)
    if (it != null) ensureNoSelfReferences(it, value, ancestors)
  }


  private def convert(value: Any): Iterable[_] = {
    import scala.collection.JavaConversions._
    value match {
      case null => null
      case value1: java.util.Map[_, _] => value1.values
      case value1: java.lang.Iterable[_] if !value.isInstanceOf[Path] => value1
      case value1: Map[_, _] => value1.values
      case value1: Iterable[_] if !value.isInstanceOf[Path] => value1
      case array: Array[AnyRef] => array.toIterable
      case _ => null
    }
  }


  private def ensureNoSelfReferences(value: Iterable[_], originalReference: Any, ancestors: util.Set[Any]): Unit = {
    if (value != null) {
      if (!ancestors.add(originalReference))
        throw new IllegalArgumentException("Iterable object is self-referencing itself")
      value.foreach(o => {
        ensureNoSelfReferences(convert(o), o, ancestors)
      })
      ancestors.remove(originalReference)
    }
  }

}
