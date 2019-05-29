package com.haima.sage.bigdata.etl.common.model.filter

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation._

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[ReAnalyzer], name = "reAnalyzer"),
  new Type(value = classOf[AnalyzerCase], name = "analyzerCase"),
  new Type(value = classOf[AnalyzerParser], name = "analyzerParser"),
  new Type(value = classOf[AnalyzerRedirect], name = "analyzerRedirect"),
  new Type(value = classOf[AnalyzerStartWith], name = "analyzerStartWith"),
  new Type(value = classOf[AnalyzerEndWith], name = "analyzerEndWith"),
  new Type(value = classOf[AnalyzerMatch], name = "analyzerMatch"),
  new Type(value = classOf[AnalyzerContain], name = "analyzerContain"),
  new Type(value = classOf[AnalyzerScriptFilter], name = "analyzerScriptFilter"),
  new Type(value = classOf[ReParser], name = "reParser"),
  new Type(value = classOf[MapCase], name = "case"),
  new Type(value = classOf[AddTags], name = "addTags"),
  new Type(value = classOf[RemoveFields], name = "removeFields"),
  new Type(value = classOf[RemoveTags], name = "removeTags"),
  new Type(value = classOf[Drop], name = "drop"),
  new Type(value = classOf[Mapping], name = "mapping"),
  new Type(value = classOf[Merger], name = "merger"),
  new Type(value = classOf[Script], name = "script"),
  new Type(value = classOf[Error], name = "error"),
  new Type(value = classOf[Error], name = "ignore"),
  new Type(value = classOf[FieldCut], name = "fieldCut"),
  new Type(value = classOf[MapRedirect], name = "redirect"),
  new Type(value = classOf[MapStartWith], name = "startWith"),
  new Type(value = classOf[MapEndWith], name = "endWith"),
  new Type(value = classOf[MapMatch], name = "match"),
  new Type(value = classOf[MapContain], name = "contain"),

  new Type(value = classOf[MapScriptFilter], name = "scriptFilter"),
  new Type(value = classOf[AddFields], name = "addFields")
))
@JsonIgnoreProperties(Array("_field", "_name"))
trait Rule extends Serializable {
  def name: String
}
