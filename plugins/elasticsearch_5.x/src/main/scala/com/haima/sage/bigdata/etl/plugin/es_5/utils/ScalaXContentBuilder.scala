package com.haima.sage.bigdata.etl.plugin.es_5.utils

import java.util.Collections

import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.xcontent.XContent
import org.elasticsearch.common.xcontent.json.JsonXContent


class ScalaXContentBuilder(xContent: XContent = JsonXContent.jsonXContent)
  extends XContentBuilder(xContent, new BytesStreamOutput(), Collections.emptySet(), Collections.emptySet()) {


  def map(values: Map[String, _]): XContentBuilder = {
    if (values == null) return nullValue
    startObject
    import scala.collection.JavaConversions._
    for (value <- values.entrySet) {
      field(value.getKey)
      unknownValue(value.getValue)
    }
    endObject
    this
  }


  protected override def unknownValue(value: Any): Unit = {
    if (value == null) {
      nullValue
      return
    }
    value match {
      case _: Map[_, _] => map(value.asInstanceOf[Map[String, _]])
      case iterable: Iterable[AnyRef@unchecked] => values(iterable.toArray)
      case array: Array[AnyRef] => values(array)
      case _ => // This is a "value" object (like enum, DistanceUnit, etc) just toString() it
        // (yes, it can be misleading when toString a Java class, but really, jackson should be used in that case)
        super.unknownValue(value)
    }
  }
}
