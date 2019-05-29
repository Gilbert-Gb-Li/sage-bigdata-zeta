package com.haima.sage.bigdata.etl.common.model.filter

trait Match[R <: Rule, C <: Case[R]] extends Rule with FieldRule with SwitchRule[R, C] {
  override def filterOne(value: String)(test: C): Boolean = value.matches(test.value)
}