package com.haima.sage.bigdata.etl.store.rules

import com.haima.sage.bigdata.etl.common.model.RuleGroup

/**
 * Created by zhhuiyan on 2015/5/28.
 */
trait RuleGroupStore extends AutoCloseable {
  def get(groupId: String): Option[RuleGroup]

  def set(group: RuleGroup): Boolean

  def delete(groupId: String): Boolean
}
