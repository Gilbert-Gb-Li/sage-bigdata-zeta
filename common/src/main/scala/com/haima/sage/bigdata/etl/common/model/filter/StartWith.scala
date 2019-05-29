package com.haima.sage.bigdata.etl.common.model.filter

trait StartWith[R <: Rule, C <: Case[R]] extends Rule with FieldRule with SwitchRule[R, C] {
  override def filterOne(value: String)(_case: C): Boolean = {
    value.startsWith(_case.value)
  }
}