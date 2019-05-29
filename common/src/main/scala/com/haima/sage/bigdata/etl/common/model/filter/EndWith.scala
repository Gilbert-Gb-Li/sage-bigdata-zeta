package com.haima.sage.bigdata.etl.common.model.filter

trait EndWith[R <: Rule, C <: Case[R]] extends Rule with FieldRule with SwitchRule[R, C] {
  override def filterOne(value: String)(test: C): Boolean = value.endsWith(test.value)
}