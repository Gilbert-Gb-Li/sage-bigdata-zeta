package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.filter.RuleProcessor

trait MapSwitchRuleProcessor[R <: SwitchRule[MapRule, MapCase]] extends RuleProcessor[RichMap, List[RichMap], R] {

  def filter: R

  private lazy val processors = filter.cases.map(real =>
    (real, Class.forName(Constants.CONF.getString("app.lexer.filter." + real.rule.name)).getConstructor(real.rule.getClass).newInstance(real.rule).asInstanceOf[RuleProcessor[RichMap, _, _ <: MapRule]]))

  private lazy val default: Option[RuleProcessor[RichMap, _, _ <: MapRule]] = filter.default.map(real =>
    Class.forName(Constants.CONF.getString("app.lexer.filter." + real.name)).getConstructor(real.getClass).newInstance(real).asInstanceOf[RuleProcessor[RichMap, _, _ <: MapRule]])


  def get(event: Map[String, Any]): Option[Any] = {
    filter.get(event)
  }

  def processor(value: String): Option[RuleProcessor[RichMap, _, _ <: MapRule]] = {
    val a = processors.find(_case => filter.filterOne(value)(_case._1)).map(_._2)

    if (a.isEmpty) {
      default
    } else {
      a
    }
  }

  override def process(event: RichMap): List[RichMap] = {
    try {
      get(event) match {

        case Some(value) if value != null =>
          processor(value.toString) match {
            case Some(processor) =>
              processor.process(event) match {
                case t: List[RichMap@unchecked] =>
                  t
                case d: RichMap =>
                  List(d)
                case _ =>
                  throw new UnsupportedOperationException("must be a rich map")
              }
            case _ =>
              List(event)
          }
        case _ =>
          /*TODO 当值不存在的时候使用default is OK ?*/
          default match {
            case Some(processor) =>
              processor.process(event) match {
                case t: List[RichMap@unchecked] =>
                  t
                case d: RichMap =>
                  List(d)
                case _ =>
                  throw new UnsupportedOperationException("must be a rich map")
              }
            case _ =>
              List(event)
          }

      }
    } catch {
      case e: Exception =>
        List(event + ("c@error" -> e.getMessage))
    }


  }
}

case class MapRedirectProcessor(filter: MapRedirect) extends MapSwitchRuleProcessor[MapRedirect]

case class MapStartWithProcessor(filter: MapStartWith) extends MapSwitchRuleProcessor[MapStartWith]

case class MapEndWithProcessor(filter: MapEndWith) extends MapSwitchRuleProcessor[MapEndWith]

case class MapContainProcessor(filter: MapContain) extends MapSwitchRuleProcessor[MapContain]

case class MapMatchProcessor(filter: MapMatch) extends MapSwitchRuleProcessor[MapMatch]

case class ScriptFilterProcessor(override val filter: MapScriptFilter) extends MapSwitchRuleProcessor[MapScriptFilter]