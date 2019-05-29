package com.haima.sage.bigdata.etl.codec

import java.io.Serializable

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

/**
  * Created by zhhuiyan on 15/4/9.
  */
object Codec {


  def apply(name: String = "line") = new Codec(name)


}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[LineCodec], name = "line"),
  new Type(value = classOf[MultiCodec], name = "multi"),
  new Type(value = classOf[JsonCodec], name = "json"),
  new Type(value = classOf[XmlCodec], name = "xml"),
  new Type(value = classOf[DelimitCodec], name = "delimit"),
  new Type(value = classOf[MatchCodec], name = "match")
))
class Codec(val name: String, filter: Option[Filter] = None) extends Serializable {

  override def toString = s"Codec($name)"
}

case class LineCodec(filter: Option[Filter] = None) extends Codec("line", filter)

case class MultiCodec(pattern: String, inStart: Option[Boolean]=None, filter: Option[Filter] = None,maxLineNum:Option[String] = Some("1000")) extends Codec("multi", filter)

case class MatchCodec(pattern: String, filter: Option[Filter] = None) extends Codec("match", filter)

case class JsonCodec(filter: Option[Filter] = None) extends Codec("json", filter)

case class XmlCodec(filter: Option[Filter] = None) extends Codec("xml", filter)

case class DelimitCodec(delimit: String, filter: Option[Filter] = None) extends Codec("delimit", filter)

