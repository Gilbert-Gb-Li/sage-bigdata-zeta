package com.haima.sage.bigdata.etl.store.rules

import com.haima.sage.bigdata.etl.common.model.{RuleUnit}

/**
 * Created by zhhuiyan on 2015/5/28.
 */
trait RuleUnitStore extends AutoCloseable {
  def get(ruleId: String): Option[RuleUnit]

  def set(rule: RuleUnit): Boolean

  def delete(ruleId: String): Boolean
}
