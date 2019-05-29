package com.haima.sage.bigdata.etl.server

import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.common.model.{Config, NetSource, Parser, RuleUnit}
import com.haima.sage.bigdata.etl.store.rules._
import com.haima.sage.bigdata.etl.utils.Mapper

import scala.io.Source

/**
  * Created by zhhuiyan on 2015/6/2.
  */
object RuleBankServiceTest extends Mapper {
  def configToRule(): Unit = {
    val reader = Source.fromFile("./src/main/resources/configJson.txt", "utf-8")
    var rules: List[RuleUnit] = List[RuleUnit]()
    reader.getLines().foreach {
      json =>
        val config = mapper.readValue[Config](json)
        val names = Map[String, (String, String, String)](
          "4302" -> ("云盾", "group5", "描述"),
          "5142" -> ("核心交换机", "group5", "描述"),
          "5146" -> ("天融信IDP", "group3", "描述"),
          "5144" -> ("天融信防火墙", "group3", "描述"),
          "5157" -> ("瑞星网络杀毒", "group3", "描述"),
          "514" -> ("冠群星辰防毒墙", "group3", "描述")
        )

        val key = config.channel.dataSource.asInstanceOf[NetSource].port.toString
        rules = RuleUnit(
          s"${config.id}",
          s"${names(key)._1}",
          s"${names(key)._2}",
          config.channel.parser.get.asInstanceOf[Parser[Rule]],
          None,
          "20150601",
          s"${names(key)._3}") :: rules
    }
    rules.foreach {
      rule =>
        println(mapper.writeValueAsString(rule))
    }
    println("====================")
    println(mapper writeValueAsString rules)
  }

  def main(args: Array[String]): Unit = {
    val bank = new RulesStore(new DBRuleUnitStore(), new DBRuleGroupStore)
    bank.output("G:\\rulebank_out.txt")
  }
}
