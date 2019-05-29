package com.haima.sage.bigdata.etl.common.model.filter

trait Contain[R <: Rule, C <: Case[R]] extends Rule with FieldRule with SwitchRule[R, C] {

  override final def filterOne(value: String)(test: C): Boolean = value.contains(test.value)

}