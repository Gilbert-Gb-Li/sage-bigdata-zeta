package com.haima.sage.bigdata.etl.utils

import com.fasterxml.jackson.core.{JsonGenerator, JsonProcessingException}
import com.fasterxml.jackson.databind.ser.{BeanPropertyWriter, BeanSerializerModifier}
import com.fasterxml.jackson.databind.{BeanDescription, JsonSerializer, SerializationConfig, SerializerProvider}


class SageBeanSerializerModifier extends BeanSerializerModifier {

  override def changeProperties(config: SerializationConfig, beanDesc: BeanDescription, beanProperties: java.util.List[BeanPropertyWriter]): java.util.List[BeanPropertyWriter] = { // 循环所有的beanPropertyWriter
    import scala.collection.JavaConversions._

    val rt = new java.util.ArrayList[BeanPropertyWriter](beanProperties.size())
    beanProperties.foreach(writer => {
      if (isArrayType(writer)) {
        writer.assignNullSerializer(arrayJsonSerializer)
      } else {
        writer.assignSerializer(defaultJsonSerializer)
      }
      rt.add(writer)
    })
    rt
  }


  // 判断是什么类型
  protected def isArrayType(writer: BeanPropertyWriter): Boolean = {

    val clazz = writer.getType
    val rt = clazz.isArrayType
    rt
  }

  protected lazy val arrayJsonSerializer: JsonSerializer[AnyRef] = new ArrayJsonSerializer
  protected lazy val defaultJsonSerializer: JsonSerializer[AnyRef] = new ArrayJsonSerializer

  class ArrayJsonSerializer extends JsonSerializer[AnyRef] {
    @throws[JsonProcessingException]
    override def serialize(value: AnyRef, generator: JsonGenerator, provider: SerializerProvider): Unit = {
      if (value == null) {
        generator.writeStartArray()
        generator.writeEndArray()
      }
      else generator.writeObject(value)
    }
  }

  class EmptySerializer extends JsonSerializer[AnyRef] {
    @throws[JsonProcessingException]
    override def serialize(value: AnyRef, generator: JsonGenerator, provider: SerializerProvider): Unit = {
      if (value == null || (value.isInstanceOf[String] && value.toString.trim == "")) {
        generator.writeNull()
      }
      else generator.writeObject(value)
    }
  }

}
