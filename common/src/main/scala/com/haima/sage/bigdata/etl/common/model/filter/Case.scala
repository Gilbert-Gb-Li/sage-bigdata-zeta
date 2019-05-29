package com.haima.sage.bigdata.etl.common.model.filter

trait Case[R <: Rule] extends Rule {
  def value: String

  def rule: R
}