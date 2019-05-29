package com.haima.sage.bigdata.etl.performance.service

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class ConditionsService(conditions: Seq[ConditionsConfig]) {

  def process (event: mutable.Map[String, Any]): mutable.Map[String, Any] = {
    conditions.foreach( c =>
     c.process(event)
    )
    event
  }

}

object ConditionsService {
  def apply(conditions: ConditionsConfig*): ConditionsService = {
    new ConditionsService(conditions)
  }
}

case class ConditionsConfig(p: Prediction,
                            c: Option[Calculations] = None,
                            result: String,
                            value: Any
                          ) {
  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = {
    val list = new ListBuffer[Any]()
    c match {
      case Some(x) =>
        val cs = x.indices
        if (cs.nonEmpty){
          cs.foreach(c => list.append(event.getOrElse(c, null)))
        }
      case None =>
    }
    list.append(value)
    p.condition match {
      case s: String if s=="default" =>
        event.getOrElseUpdate(result, value)
        event
      case r: String =>
        val regex = r.r
        regex.findFirstIn(event.getOrElse(p.index, "").toString) match {
          case Some(_) =>
            c match {
              case Some(x) => event += (result -> x.f(list))
              case None => event += (result -> value)
            }
          case None => event
        }
      case true => c match {
        case Some(x) => event += (result -> x.f(list))
        case None => event += (result -> value)
      }
      case _ => event
    }
  }
}

case class Prediction(index: String = "", condition: Any = "default")

case class Calculations(indices: Seq[String] = Nil,
                        f: Seq[Any] => Any = x => if (x.head!=null) x.head else x(1))