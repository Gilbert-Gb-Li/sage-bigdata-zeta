package com.haima.sage.bigdata.etl.common.model.filter

trait SwitchRule[R <: Rule, C <: Case[R]] extends Rule with WithGet {
  def cases: Array[C]

  def default: Option[R] = None

  def filterOne(value: String)(test: C): Boolean

}