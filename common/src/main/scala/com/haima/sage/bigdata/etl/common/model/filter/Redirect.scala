package com.haima.sage.bigdata.etl.common.model.filter

trait Redirect[R <: Rule, C <: Case[R]] extends FieldRule with SwitchRule[R, C] {

  override def filterOne(value: String)(_case: C): Boolean = {
    _case.value.equals(value)
  }
}