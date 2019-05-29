package com.haima.sage.bigdata.etl.performance.service

import scala.collection.mutable

class BaseConditionService(conditions: Seq[BaseConditionConfig]) {

  def process (event: mutable.Map[String, Any]):Unit = {
    conditions.foreach( c =>
      c.process(event)
    )
  }

}

object BaseConditionService {
  def apply(conditions: BaseConditionConfig*): BaseConditionService = {
    new BaseConditionService(conditions)
  }

}

case class BaseConditionConfig(f: mutable.Map[String, Any] => Unit){
  def process (event: mutable.Map[String, Any]): Unit ={
    f(event)
  }

}