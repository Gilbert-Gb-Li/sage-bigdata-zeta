package com.haima.sage.bigdata.etl.store.rules

import java.io.{File, PrintWriter}

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{RuleGroup, RuleUnit, Rules}
import com.haima.sage.bigdata.etl.utils.{Logger, Mapper}

import scala.io.Source


/**
  * Created by zhhuiyan on 2015/6/1.
  */
class RulesStore(ruleDB: DBRuleUnitStore, groupDB: DBRuleGroupStore) extends Mapper with Logger {



   private var rulesCache: Map[String, RuleUnit] = _
  private var groupsCache: Map[String, RuleGroup] = _

  //  private def rulesCacheMap = rulesCache.groupBy(_.groupid)
  //  private def groupsCacheMap = groupsCache.groupBy(_.parent)


  def groupSave(group: RuleGroup): Unit = {
    group.groups match {
      case None =>
      case Some(gs: List[RuleGroup]) =>
        gs.foreach(groupSave)

    }
    group.rules match {
      case None =>
      case Some(rules: List[RuleUnit]) =>
        rules.foreach(ruleDB.set)
    }
  }

  def input(path: String): Unit = {
    try {
      val reader = Source.fromFile(path, "utf-8")
      logger.info(s"Initializing rule bank from $path...")
      val log = logger
      val groups = mapper.readValue[List[RuleGroup]](reader.getLines().mkString(""))
      groups.foreach { group =>
        groupSave(group)
      }
      /* reader.getLines().foreach {
         json =>
           val data = json.split("=", 2)
           data(0) match {
             case "group" =>
               try {
                 import com.haima.sage.bigdata.etl.common.model.RuleBankJsonProtocol._
                 val group = data(1).parseJson.convertTo[RuleGroup]
                 groupDB.set(group)
               } catch {
                 case e: Exception =>
                   logger.error(s"Group input failed:$json", e)
               }
             case "rule" =>
               try {
                 import com.haima.sage.bigdata.etl.common.model.RuleBankJsonProtocol._
                 val rule = data(1).parseJson.convertTo[RuleUnit]
                 ruleDB.set(rule)
               } catch {
                 case e: Exception =>
                   logger.error(s"Rule input failed:$json", e)
               }
             case _ =>
               logger.warn(s"Input failed:$json")
           }
       }*/
      log.info("Rule bank initialized finished .")
      refreshCache()
      reader.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.warn(s"Initialized rule bank from $path failed : ${e.getMessage}")
    }
  }

  def output(path: String): Unit = {
    try {
      logger.info("rule bank output start")
      val writer = new PrintWriter(new File(path))
      groupDB.getAll().foreach {
        g =>
          try {
            writer.println(s"group=${mapper.writeValueAsString(g)}")
          } catch {
            case e: Exception =>
              logger.error(s"output failed:$g", e)
          }
      }
      ruleDB.all().foreach {
        r =>
          try {
            writer.println(s"rule=${mapper.writeValueAsString(r)}")
          } catch {
            case e: Exception =>
              logger.error(s"output failed:$r", e)
          }
      }
      writer.close()
      logger.info(s"rule bank output to $path finished")
    } catch {
      case e: Exception =>
        logger.error(s"output to $path  failed", e)
    }
  }

  // search all RuleGroup
  def groups(): List[RuleGroup] = {
    groupsCache.values.toList

  }

  def all(): List[Rules] = {
    groupsCache.values.toList

  }

  // search RuleGroup
  def group(id: String): Option[RuleGroup] = {
    groupsCache.get(id)
  }

  // delete RuleGroup
  def deleteGroup(id: String): Boolean = {
    val gr = groupDB.delete(id)
    val rr = ruleDB.deleteByGroup(id)
    refreshCache()
    gr && rr
  }

  // add new RuleGroup
  def addGroup(group: RuleGroup): Boolean = {
    val result = groupDB.set(group)
    if (result) refreshGroupCache()
    result
  }

  def add(rules: Rules): Boolean = {
    rules match {
      case group: RuleGroup =>
        addGroup(group)
      case unit: RuleUnit =>
        addUnit(unit)
    }
  }

  def delete(rules: Rules): Boolean = {
    rules match {
      case group: RuleGroup =>
        deleteGroup(rules.id)
      case unit: RuleUnit =>
        deleteUnit(rules.id)
    }
  }

  def delete(id: String): Boolean = {
    get(id) match {
      case Some(group: RuleGroup) =>
        deleteGroup(group.id)
      case Some(unit: RuleUnit) =>
        deleteUnit(unit.id)
      case _ =>
        false
    }
  }

  // add new Rule
  def addUnit(rule: RuleUnit): Boolean = {
    val result = ruleDB.set(rule)
    if (result) refreshRulesCache()
    result
  }

  // search Rule
  def rule(id: String): Option[RuleUnit] = rulesCache.get(id)

  def get(id: String): Option[Rules] =
    groupsCache.get(id) match {
      case Some(group) =>
        Some(group)
      case None =>
        rulesCache.get(id)
    }


  // search all Rule
  def rules: List[RuleUnit] = rulesCache.values.toList

  // delete Rule
  def deleteUnit(id: String): Boolean = {
    val result = ruleDB.delete(id)
    if (result) refreshRulesCache()
    result
  }

  private def initial(): Unit = {
    input(Constants.getApiServerConf(Constants.RULE_BANK_FILE_PATH))
  }


  private def refreshCache(): Unit = {
    refreshGroupCache()
    refreshRulesCache()
  }


  private def subGroup(parent: String, groups: List[RuleGroup]): Option[List[RuleGroup]] = {
    groups.filter(_.parent match {
      case Some(p) =>
        p == parent
      case None =>
        false
    }).map {
      case group =>
        group.copy(groups = subGroup(group.id, groups.toList), rules = subRules(group.id))
    } match {
      case Nil =>
        None
      case obj =>
        Some(obj)
    }

  }

  private def subRules(parent: String): Option[List[RuleUnit]] = {
    rulesCache.values.filter(_.groupId == parent).toList match {
      case Nil =>
        None
      case obj =>
        Some(obj)
    }
  }


  private def refreshGroupCache(): Unit = {
    rulesCache = ruleDB.all().map(rule => (rule.id, rule)).toMap
    val groups = groupDB.getAll().map(rule => (rule.id, rule)).toMap
    val roots = groups.filter(_._2.parent.isEmpty)

    groupsCache = roots.map {
      case (key, value) =>
        (key, value.copy(groups = subGroup(key, groups.values.toList), rules = subRules(key)))
    }
  }

  private def refreshRulesCache(): Unit = {
    rulesCache = ruleDB.all().map(rule => (rule.id, rule)).toMap
  }

  //TODO load rules  initial()
}
