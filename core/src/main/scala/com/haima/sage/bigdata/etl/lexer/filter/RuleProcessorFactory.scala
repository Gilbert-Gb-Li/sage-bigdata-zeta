package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.filter.RuleProcessor

/**
  * Created by evan on 18-3-23.
  */
object RuleProcessorFactory extends Serializable {
  def getProcessor(rule: MapRule): RuleProcessor[RichMap, _, _ <: MapRule] = {
    rule match {
      case _: AddFields =>
        new AddFieldsProcessor(rule.asInstanceOf[AddFields])
      case _: ReParser =>
        ReParserProcessor(rule.asInstanceOf[ReParser])
      case _: AddTags =>
        new AddTagsProcessor(rule.asInstanceOf[AddTags])
      case _: Script =>
        new ScriptProcessor(rule.asInstanceOf[Script])
      case _: ByKnowledge =>
        new ByKnowledgeProcessor(rule.asInstanceOf[ByKnowledge])
      case _: RemoveFields =>
        new RemoveFieldsProcessor(rule.asInstanceOf[RemoveFields])
      case _: RemoveTags =>
        new RemoveTagsProcessor(rule.asInstanceOf[RemoveTags])
      case _: Drop =>
        new DropProcessor(rule.asInstanceOf[Drop])
      case _: Mapping =>
        new MappingProcessor(rule.asInstanceOf[Mapping])
      case _: Merger =>
        new MergerProcessor(rule.asInstanceOf[Merger])
      case _: Error =>
        new ErrorProcessor(rule.asInstanceOf[Error])
      case _: Ignore =>
        new IgnoreProcessor(rule.asInstanceOf[Ignore])
      case _: FieldCut =>
        new FieldCutProcessor(rule.asInstanceOf[FieldCut])
      case _: MapRedirect =>
        MapRedirectProcessor(rule.asInstanceOf[MapRedirect])
      case _: MapStartWith =>
         MapStartWithProcessor(rule.asInstanceOf[MapStartWith])
      case _: MapEndWith =>
         MapEndWithProcessor(rule.asInstanceOf[MapEndWith])
      case _: MapMatch =>
         MapMatchProcessor(rule.asInstanceOf[MapMatch])
      case _: MapContain =>
         MapContainProcessor(rule.asInstanceOf[MapContain])
      case _: MapScriptFilter =>
         ScriptFilterProcessor(rule.asInstanceOf[MapScriptFilter])
      //      case _:MapCase =>
    }
  }

  def getProcessors(f: Filter): List[RuleProcessor[RichMap, _, _ <: MapRule]] = {
    f.rules.map(getProcessor).toList
  }
}
