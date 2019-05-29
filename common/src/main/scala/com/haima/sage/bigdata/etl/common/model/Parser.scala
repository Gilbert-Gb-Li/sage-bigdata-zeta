package com.haima.sage.bigdata.etl.common.model

import java.io.Serializable

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.haima.sage.bigdata.etl.common.model.filter.{MapRule, Rule}


/**
  * Created by zhhuiyan on 15/4/9.
  */
object Parser {

  def apply(name: String = "nothing",
            filter: Array[MapRule] = Array(),
            metadata: Option[List[(String, String, String)]] = None): Parser[MapRule] = {
    val _filter = filter
    val _name = name
    val _metadata = metadata
    new Parser[MapRule]() {
      override val name: String = _name
      val filter: Array[MapRule] = _filter
      val metadata: Option[List[(String, String, String)]] = _metadata
    }

  }
}

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[TransferParser], name = "transfer"),
  new Type(value = classOf[NothingParser], name = "nothing"),
  new Type(value = classOf[CefParser], name = "cef"),
  new Type(value = classOf[XmlParser], name = "xml"),
  new Type(value = classOf[JsonParser], name = "json"),

  new Type(value = classOf[AvroParser], name = "avro"),
  new Type(value = classOf[ProtobufParser], name = "protobuf"),
  new Type(value = classOf[Regex], name = "regex"),
  new Type(value = classOf[Delimit], name = "delimit"),
  new Type(value = classOf[DelimitWithKeyMap], name = "delimitWithKeyMap"),
  new Type(value = classOf[SelectParser], name = "select"),
  new Type(value = classOf[ChainParser], name = "chain")
))
trait Parser[T <: Rule] extends Serializable {
  val name: String = getClass.getSimpleName.toLowerCase.replace("parser", "")
  val filter: Array[T]
  val metadata: Option[List[(String, String, String)]]

}

case class TransferParser(override val filter: Array[MapRule] = Array(),
                          override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]

case class NothingParser(override val filter: Array[MapRule] = Array(),
                         override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]

case class CefParser(override val filter: Array[MapRule] = Array(),
                     override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]

case class XmlParser(override val filter: Array[MapRule] = Array(),
                     override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]
case class AvroParser(schema:String,override val filter: Array[MapRule] = Array(),
                     override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]

case class ProtobufParser(schema:String,override val filter: Array[MapRule] = Array(),
                     override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]

case class JsonParser(override val filter: Array[MapRule] = Array(),
                      override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]

case class Regex(value: String,
                 override val filter: Array[MapRule] = Array(),
                 override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]


case class Delimit(delimit: Option[String] = Some(","),
                   fields: Array[String],
                   override val filter: Array[MapRule] = Array(),
                   override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]

case class DelimitWithKeyMap(delimit: Option[String] = Some(","),
                             tab: Option[String] = Some("="),
                             override val filter: Array[MapRule] = Array(),
                             override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule] {
  override val name = "delimitWithKeyMap"
}


case class SelectParser(parsers: Array[Parser[MapRule]],
                        override val filter: Array[MapRule] = Array(),
                        override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]

case class ChainParser(parsers: Array[Parser[MapRule]], override val filter: Array[MapRule] = Array(),
                       override val metadata: Option[List[(String, String, String)]] = None) extends Parser[MapRule]
