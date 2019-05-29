package com.haima.sage.bigdata.etl.codec

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonSubTypes, JsonTypeInfo}
import com.haima.sage.bigdata.etl.common.model.Event

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[StartWith], name = "startsWith"),
  new Type(value = classOf[EndWith], name = "endWith"),
  new Type(value = classOf[Match], name = "match"),
  new Type(value = classOf[Contain], name = "contain")

))
abstract class Filter(val name: String) extends Serializable {
  def filter(event: Event): Boolean
}

case class StartWith(value: String) extends Filter("startsWith") {
  override def filter(event: Event): Boolean = {
    event.content.startsWith(value)
  }
}

case class EndWith(value: String) extends Filter("endWith") {
  override def filter(event: Event): Boolean = {
    event.content.endsWith(value)
  }
}

case class Match(value: String) extends Filter("match") {
  override def filter(event: Event): Boolean = {
    event.content.matches(value)
  }
}

case class Contain(value: String) extends Filter("contain") {
  override def filter(event: Event): Boolean = {
    event.content.contains(value)
  }
}

