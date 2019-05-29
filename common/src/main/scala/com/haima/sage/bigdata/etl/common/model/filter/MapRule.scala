package com.haima.sage.bigdata.etl.common.model.filter

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.haima.sage.bigdata.etl.common.model.{Parser => MParser}
import com.haima.sage.bigdata.etl.utils.Mapper


@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[ReParser], name = "reParser"),
  new Type(value = classOf[MapCase], name = "case"),
  new Type(value = classOf[AddTags], name = "addTags"),
  new Type(value = classOf[Gzip], name = "gzip"),
  new Type(value = classOf[Gunzip], name = "gunzip"),
  new Type(value = classOf[RemoveFields], name = "removeFields"),
  new Type(value = classOf[RemoveTags], name = "removeTags"),
  new Type(value = classOf[Drop], name = "drop"),
  new Type(value = classOf[Extends], name = "extends"),
  new Type(value = classOf[List2Map], name = "list2map"),
  new Type(value = classOf[Mapping], name = "mapping"),
  new Type(value = classOf[Merger], name = "merger"),
  new Type(value = classOf[Script], name = "script"),
  new Type(value = classOf[Error], name = "error"),
  new Type(value = classOf[Ignore], name = "ignore"),
  new Type(value = classOf[Replace], name = "replace"),
  new Type(value = classOf[FieldMulti], name = "fieldMulti"),
  new Type(value = classOf[FieldAdditive], name = "fieldAdditive"),
  new Type(value = classOf[FieldCut], name = "fieldCut"),
  new Type(value = classOf[MapRedirect], name = "redirect"),
  new Type(value = classOf[MapStartWith], name = "startWith"),
  new Type(value = classOf[MapEndWith], name = "endWith"),
  new Type(value = classOf[MapMatch], name = "match"),
  new Type(value = classOf[MapContain], name = "contain"),

  new Type(value = classOf[MapScriptFilter], name = "scriptFilter"),
  new Type(value = classOf[AddFields], name = "addFields"),

  new Type(value = classOf[ByKnowledge], name = "byKnowledge")
))
@JsonIgnoreProperties(Array("_field", "_name"))
trait MapRule extends Rule {
  override val name: String = {
    val _name = this.getClass.getSimpleName.replaceAll("Map", "")
    _name.updated(0, _name.charAt(0).toLower)
  }
}

trait MapFieldRule extends MapRule with FieldRule

case class Mapping(fields: Map[String, String]) extends MapRule {
  override val name: String = "mapping"


}

case class Extends(field: String) extends MapRule {
  override val name: String = "extends"
}

case class List2Map(field: String,fields: String) extends MapRule {
  override val name: String = "list2map"

}

case class Script(script: String, `type`: String = "scala" //options scala,js
                 ) extends MapRule

case class Merger(fields: Array[String], field: String, sep: Option[String] = None) extends MapRule

case class AddTags(fields: Set[String]) extends MapRule

case class Gzip(fields: Set[String]) extends MapRule

case class Gunzip(fields: Set[String]) extends MapRule

case class RemoveFields(fields: Set[String]) extends MapRule

case class Replace(field: String, from: String, to: String) extends MapRule

case class FieldMulti(field: String, value: Double) extends MapRule

case class FieldAdditive(field: String, value: Double) extends MapRule

case class RemoveTags(fields: Set[String]) extends MapRule

case class MapCase(value: String, rule: MapRule) extends MapRule with Case[MapRule]

case class AddFields(fields: Map[String, String]) extends MapRule

case class ByKnowledge(id: String, column: String = null, value: String = null, isScript: Boolean = false) extends MapRule

/*
@JsonDeserialize(using = classOf[CustomDeserializer])
@JsonSerialize(using = classOf[CustomSerializer])
*/
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.DEFAULT)
@JsonIgnoreProperties(Array("mapper", "_parser"))
case class ReParser(field: Option[String] = None,
                    parser: MParser[MapRule],
                    ref: Option[String] = None) extends MapRule


import com.fasterxml.jackson.core.{JsonGenerator, JsonProcessingException}
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer

class CustomSerializer(val t: Class[ReParser]) extends StdSerializer[ReParser](t) {
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


import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer

class CustomDeserializer(val vc: Class[ReParser]) extends StdDeserializer[ReParser](vc) {
  def this() {
    this(null)
  }

  val tools = new Mapper {}

  @throws[JsonProcessingException]
  def deserialize(jsonparser: JsonParser, context: DeserializationContext): ReParser = {
    val data = jsonparser.readValueAs[Map[String, String]](classOf[Map[String, String]])

    ReParser(data.get("field"), tools.mapper.readValue[MParser[MapRule]](tools.mapper.writeValueAsString(data("parser"))), data.get("ref"))
  }
}

case class Drop() extends MapRule

case class Ignore() extends MapRule

case class Error() extends MapRule

case class FieldCut(field: String, from: Int, limit: Int) extends MapFieldRule


case class MapRedirect(field: String, override val cases: Array[MapCase], override val default: Option[MapRule] = None)
  extends Redirect[MapRule, MapCase]
    with MapFieldRule
    with SwitchRule[MapRule, MapCase]


case class MapStartWith(field: String, override val cases: Array[MapCase], override val default: Option[MapRule] = None)
  extends StartWith[MapRule, MapCase] with MapFieldRule with SwitchRule[MapRule, MapCase]


case class MapEndWith(field: String, override val cases: Array[MapCase], override val default: Option[MapRule] = None)
  extends EndWith[MapRule, MapCase] with MapFieldRule with SwitchRule[MapRule, MapCase]

case class MapMatch(field: String, override val cases: Array[MapCase], override val default: Option[MapRule] = None)
  extends Match[MapRule, MapCase] with MapFieldRule with SwitchRule[MapRule, MapCase]

case class MapContain(field: String, override val cases: Array[MapCase], override val default: Option[MapRule] = None)
  extends Contain[MapRule, MapCase] with SwitchRule[MapRule, MapCase] with MapFieldRule


case class MapScriptFilter(script: String, override val `type`: String = "scala", //options scala,js
                           override val cases: Array[MapCase], override val default: Option[MapRule] = None)
  extends MapRule with ScriptFilter[MapRule, MapCase]
