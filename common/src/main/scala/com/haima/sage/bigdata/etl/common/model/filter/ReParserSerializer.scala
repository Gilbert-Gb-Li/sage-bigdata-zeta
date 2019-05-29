package com.haima.sage.bigdata.etl.common.model.filter

import com.fasterxml.jackson.core.{JsonGenerator, JsonProcessingException}
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.haima.sage.bigdata.etl.utils.Mapper

class ReParserSerializer(val t: Class[ReParser]) extends StdSerializer[ReParser](t) {
  def this() {
    this(null)
  }

  @throws[JsonProcessingException]
  override def serialize(item: ReParser, generator: JsonGenerator, provider: SerializerProvider): Unit = {
    serializeWithType(item, generator, provider, null)
  }

  override def serializeWithType(item: ReParser, gen: JsonGenerator, serializers: SerializerProvider, typeSer: TypeSerializer): Unit = {
    item.ref match {
      case Some(_) =>
        gen.writeObject(Map("name" -> item.name, "field" -> item.field, "parser" -> item.parser, "ref" -> item.ref))
      case _ =>
        gen.writeObject(Map("name" -> item.name, "field" -> item.field, "parser" -> item.parser))
    }

  }
}