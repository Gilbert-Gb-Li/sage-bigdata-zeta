package com.haima.sage.bigdata.etl.performance.test

import com.haima.sage.bigdata.etl.performance.service.{BaseConditionConfig, BaseConditionService}

import scala.collection.mutable

object BaseConditionServiceTest extends App {

  var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
  dst += ("registration_date" -> "2017-8")
  dst += ("state_tmp" -> ".没有找到相关数据...")
  private val baseConditionService = BaseConditionService(
    BaseConditionConfig(event => {
      val registration_date = event.getOrElse("registration_date", "unknown").toString
      val pattern = "\\d{4}-\\d{1,2}".r
      pattern.findFirstIn(registration_date) match {
        case Some(x) => event += ("registration_date" -> x)
        case None => event += ("registration_date" -> "unknown")
      }
    }),    BaseConditionConfig(event => {
      val car_name = event.getOrElse("car_name", "").toString
      val pattern = "^\\s.*".r
      pattern.findFirstIn(car_name) match {
        case Some(_) => event += ("is_strict" -> 1)
        case None => event += ("is_strict" -> 0)}
    }),
    BaseConditionConfig(event => {
      val state_tmp = event.getOrElse("state_tmp", "").toString
      val offShelf = "没有找到相关数据"
      val saled = "全部车源信息"
      if (state_tmp.contains(offShelf)) {
        event += ("state" -> 1)
      } else if (state_tmp.contains(saled)) {
        event += ("state" -> 2)
      } else {
        event += ("state" -> 0)
      }
    }),
    BaseConditionConfig(event => {
      event.getOrElseUpdate("service_percentage", 0)
      event.getOrElseUpdate("is_new", 0)
    })
  )

  baseConditionService.process(dst)
  dst.foreach(m => println(m._1, m._2))


}
