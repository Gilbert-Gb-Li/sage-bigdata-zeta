package com.haima.sage.bigdata.etl.common.model.filter

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.haima.sage.bigdata.etl.common.model.{Analyzer, Parser}

/**
  * Created by ChengJi on 2017/8/16.
  */
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
  new Type(value = classOf[AnalyzerScriptFilter], name = "analyzerScriptFilter")
))
@JsonIgnoreProperties(Array("_field", "_name", "typeInfo", "_parser", "store"))
trait AnalyzerRule extends Rule {
  override val name: String = {
    val _name = this.getClass.getSimpleName
    _name.updated(0, _name.charAt(0).toLower)
  }
}

//界面的
@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[SplitTokenizer], name = "split"),
  new Type(value = classOf[AnsjTokenizer], name = "ansj")
 ))
trait Tokenizer  { //API接口 供页面或者第三方调用的
  def name: String
}
case class SplitTokenizer(separator:String) extends Tokenizer{
 val name: String="split"  //split这个名字一个是供前端调用，一个是用于template的配置名
}

case class AnsjTokenizer() extends Tokenizer{
  val name: String="ansj"  
}


@JsonIgnoreProperties(Array("_field", "_name", "store", "typeInfo"))
case class AnalyzerParser(parser: Option[Parser[MapRule]], ref: Option[String] = None) extends AnalyzerRule


/*
*@JsonDeserialize(using = classOf[CustomDeserializer])
*@JsonSerialize(using = classOf[CustomSerializer])
*/
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.DEFAULT)
@JsonIgnoreProperties(Array("mapper", "logger"))
case class ReAnalyzer(analyzer: Option[Analyzer],
                      ref: Option[String] = None) extends AnalyzerRule


trait AnalyzerFieldRule extends FieldRule with AnalyzerRule


case class AnalyzerCase(value: String, rule: AnalyzerRule) extends AnalyzerRule with Case[AnalyzerRule]

case class AnalyzerRedirect(field: String, override val cases: Array[AnalyzerCase], override val default: Option[AnalyzerRule] = None)
  extends AnalyzerFieldRule
    with Redirect[AnalyzerRule, AnalyzerCase]


case class AnalyzerStartWith(field: String, override val cases: Array[AnalyzerCase], override val default: Option[AnalyzerRule] = None)
  extends AnalyzerFieldRule
    with StartWith[AnalyzerRule, AnalyzerCase]


case class AnalyzerEndWith(field: String, override val cases: Array[AnalyzerCase], override val default: Option[AnalyzerRule] = None)
  extends AnalyzerFieldRule
    with EndWith[AnalyzerRule, AnalyzerCase]

case class AnalyzerMatch(field: String, override val cases: Array[AnalyzerCase], override val default: Option[AnalyzerRule] = None)
  extends AnalyzerFieldRule
    with Match[AnalyzerRule, AnalyzerCase]

case class AnalyzerContain(field: String, override val cases: Array[AnalyzerCase], override val default: Option[AnalyzerRule] = None)
  extends AnalyzerFieldRule
    with Contain[AnalyzerRule, AnalyzerCase]


case class AnalyzerScriptFilter(override val script: String, override val `type`:String="scala",//options scala,js
                                override val cases: Array[AnalyzerCase], override val default: Option[AnalyzerRule] = None)
  extends ScriptFilter[AnalyzerRule, AnalyzerCase] with AnalyzerRule

