package com.haima.sage.bigdata.etl.common.model

import com.haima.sage.bigdata.etl.codec._
import com.haima.sage.bigdata.etl.common.model.filter.Rule

/**
  * Created by zhhuiyan on 2015/5/28.
  */

abstract class Rules(val id: String, val name: String, val description: String) extends Serializable

case class RuleUnit(override val id: String, override val name: String, groupId: String, parser: Parser[Rule], codec: Option[Codec], version: String, override val description: String) extends Rules(id, name, description)

case class RuleGroup(override val id: String, override val name: String, parent: Option[String], override val description: String, var groups: Option[List[RuleGroup]], var rules: Option[List[RuleUnit]]) extends Rules(id, name, description)



