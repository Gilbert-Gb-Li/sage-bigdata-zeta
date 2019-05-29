package com.haima.sage.bigdata.etl.writer

import akka.actor.{Actor, ActorRef}
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.utils.Logger

import scala.collection.mutable

/**
  * Created by zhhuiyan on 2016/11/9.
  */
trait BatchProcess extends Actor {

  private lazy val logger = Logger("com.haima.sage.bigdata.etl.writer.BatchProcess").logger
  private val batches: mutable.Map[ActorRef, Map[Long, Int]] = mutable.Map()

  def tail(num: Int): Unit

  def process(batch: Long): Unit = {
    val key = sender()
    batches.put(key, batches.get(key) match {
      case None =>
        Map(batch -> 1)
      case Some(d: Map[Long, Int]) =>
        d + (batch -> (d.getOrElse(batch, 0) + 1))
    })
  }

  def write(batch: Long, ts: List[RichMap]): Unit = {
    process(batch)
    ts.foreach(t => {
      write(t)
    })
  }

  def write(t: RichMap): Unit

  /**
    * ref report to
    **/
  def report(send: Option[String] = None): Unit = {
    if (batches.nonEmpty) {
      tail(batches.values.map(_.values.sum).sum)
      send match {
        case Some(path: String) if path.trim.length > 0 =>
          batches.foreach(v => {
            v._1 ! (path, v._2)
          })
        case _ =>
          batches.foreach(v => {
            v._1 ! (self.path.name, v._2)
          })
      }
      batches.clear()
    }


  }

}
