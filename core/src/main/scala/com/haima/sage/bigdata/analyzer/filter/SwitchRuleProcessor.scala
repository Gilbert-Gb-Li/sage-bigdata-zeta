package com.haima.sage.bigdata.analyzer.filter

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{AnalyzerModel, RichMap}
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import com.haima.sage.bigdata.etl.utils.ClassUtils

trait SwitchRuleProcessor[R <: SwitchRule[AnalyzerRule, AnalyzerCase], T] extends RuleProcessor[T, List[T], R] {
  def filter: R

  def engine(): AnalyzerModel.AnalyzerModel

  def get(event: RichMap): Option[Any] = filter.get(event)

  def one(event: RichMap)(test: AnalyzerCase): Boolean = {
    get(event) match {
      case Some(v) if v != null =>
        filter.filterOne(v.toString)(test)
      case _ =>
        false
    }
  }

  def oneElse(event: RichMap): Boolean = {
    get(event) match {
      case Some(v) if v != null =>
        !filter.cases.exists(test => filter.filterOne(v.toString)(test))
      case _ =>
        true
    }

  }
  private lazy val classes = if (AnalyzerModel.STREAMING == engine()) {
    ClassUtils.subClass(classOf[RuleProcessor[_, _, _]], name = "com.haima.sage.bigdata.analyzer.streaming.filter")
  } else {
    ClassUtils.subClass(classOf[RuleProcessor[_, _, _]], name = "com.haima.sage.bigdata.analyzer.modeling.filter")
  }
  lazy val cases: Array[(AnalyzerCase, RuleProcessor[T, List[T], _ <: AnalyzerRule])] = filter.cases.map(real => {
    val clazz=real.rule.getClass
    (real,classes(clazz).getConstructor(clazz).newInstance(real.rule).asInstanceOf[RuleProcessor[T, List[T], _ <: AnalyzerRule]])
  })
  lazy val default: Option[RuleProcessor[T, List[T], _ <: AnalyzerRule]] = filter.default.map(real => {
    val clazz=real.getClass
    classes(clazz).getConstructor(clazz).newInstance(real).asInstanceOf[RuleProcessor[T, List[T], _ <: AnalyzerRule]]
  })

  implicit def to(stream: T): {def filter(fun: RichMap => Boolean): T}

  override def process(stream: T): List[T] = {
    /*
    * 每一种case生成一组stream返回,如果有default 也返回一个组流
    * swith(get(key)){
    * case rule1=>
    *   stream1
    * case rule2=>
    *   stream2
    case rule..=>
    *   stream..
    * default =>
    * stream-d
    * }
    **/
    val _streams = cases.map(test => {
      test._2.process(stream.filter(event => one(event)(test._1)))
    }).toList
    val streams = if (_streams.isEmpty) {
      List[T]()
    } else {
      _streams.reduce {
        (first, seconds) =>
          first.++(seconds)
      }
    }

    default match {
      case Some(rule) =>
        rule.process(stream.filter(event => oneElse(event))) ++ streams
      case _ =>
        streams
    }
  }
}

trait RedirectProcessor[T] extends SwitchRuleProcessor[AnalyzerRedirect, T]

trait EndWithProcessor[T] extends SwitchRuleProcessor[AnalyzerEndWith, T]

trait StartWithProcessor[T] extends SwitchRuleProcessor[AnalyzerStartWith, T]

trait MatchProcessor[T] extends SwitchRuleProcessor[AnalyzerMatch, T]

trait ContainProcessor[T] extends SwitchRuleProcessor[AnalyzerContain, T]

trait ScriptFilterProcessor[T] extends SwitchRuleProcessor[AnalyzerScriptFilter, T]