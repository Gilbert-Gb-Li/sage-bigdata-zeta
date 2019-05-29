package com.haima.sage.bigdata.etl.filter

import com.haima.sage.bigdata.etl.common.model.filter.Rule

trait RuleProcessor[F, T, R <: Rule] extends Serializable {
  def filter: R

  def process(event: F): T

}