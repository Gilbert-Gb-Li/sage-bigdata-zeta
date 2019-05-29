/*
package com.haima.sage.bigdata.etl.utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import oi.thekraken.grok.api.{Grok, Match}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool, PooledObject}

import scala.collection.JavaConversions._

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/8/13 9:15.
  */
object TimestampMaker extends BasePooledObjectFactory[Maker] {
  private val pool: ObjectPool[Maker] = {
    val genericObjectPool = new GenericObjectPool[Maker](TimestampMaker)
    genericObjectPool.setMinIdle(10)
    genericObjectPool.setMaxIdle(100)
    genericObjectPool.setMaxTotal(2000)
    genericObjectPool.setMinEvictableIdleTimeMillis(10)
    genericObjectPool
  }


  def apply(): Maker = {
    pool.borrowObject()
  }

  def wrap(obj: Maker): PooledObject[Maker] = {
    new DefaultPooledObject[Maker](obj)
  }

  def close(obj: Maker): Unit = {
    pool.returnObject(obj)
  }

  def create(): Maker = new Maker
}

class Maker() {
  private val TIMESTAMP: String = "@timestamp"
  private val DATE: String = "date"
  private val TIME: String = "time"
  private final val format = new SimpleDateFormat("yyyyMMdd HH:mm:ss.S")
  private final val grok: Grok = Grok.getInstance("%{DATESTAMP}")


  private def mkString(default: String, func: (String) => String)(value: Option[Any]): String = {
    value match {
      case None =>
        default
      case Some(day) =>
        func(day.toString)
    }
  }

  private val mk00 = mkString("00", (value: String) => {
    if (value.length == 1) "0" + value else value
  }) _

  @throws(classOf[ParseException])
  private def makeIndexSuffix(log: Map[String, Any]): Date = {
    var result: String = ""
    val year = log.get("_year")
    val month = log.get("_month")
    val month_num = log.get("_month_num")
    val month_num2 = log.get("_month_num2")
    val month_day = log.get("_month_day")

    val hour = log.get("_hour")
    val minute = log.get("_minute")
    val second = log.get("_second")
    // val mcro_second = log.get("_mcro_second")

    year match {
      case Some(a: String) =>
        a.length match {
          case 2 =>
            result += ("20" + a)
          case 4 =>
            result += a
          case _ =>
            result += a
        }
      case obj =>
        result += Calendar.getInstance().get(Calendar.YEAR)

      //throw new ParseException("year you set @timestamp is null", 1)


    }
    result += monthComputer(month, month_num, month_num2)
    month_day match {
      case None =>
        throw new ParseException("month_day you set @timestamp is null", 1)
      case value@Some(day) =>
        result += mk00(value)
    }
    val d = format.parse(s"$result ${mk00(hour)}:${mk00(minute)}:${
      mkString("00.000", (value: String) => {
        value.length match {
          case 1 =>
            "0" + value + ".000"
          case 2 =>
            value + ".000"
          case _ =>
            val values = value.split(":")
            if (values.length == 2) {
              values(0) + "." + values(1)
            } else {
              value
            }

        }
      })(second)
    }")
    if (Calendar.getInstance().getTime.before(d)) {
      val calendar = Calendar.getInstance()
      calendar.setTime(d)
      calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) - 1)
      calendar.getTime
    } else {
      d
    }

  }

  @throws(classOf[ParseException])
  private def monthComputer(month: Option[Any], month_num: Option[Any], month_num2: Option[Any]): String = {
    (month, month_num, month_num2) match {
      case (None, value@Some(c), None) =>
        mk00(value)
      case (None, None, value@Some(c)) =>
        mk00(value)
      case (Some(c), None, None) =>
        c.toString.trim.toLowerCase.substring(0, 3) match {
          case "jan" =>
            "01"
          case "feb" =>
            "02"
          case "mar" =>
            "03"
          case "apr" =>
            "04"
          case "may" =>
            "05"
          case "jun" =>
            "06"
          case "jul" =>
            "07"
          case "aug" =>
            "08"
          case "sep" =>
            "09"
          case "oct" =>
            "10"
          case "nov" =>
            "11"
          case "dec" =>
            "12"
        }
      case _ =>
        throw new ParseException("month you set @timestamp is null", 1)

    }

  }

  /** import scala.collection.JavaConversions._hen have error use new Date();
    *
    * @param log Map<String, Object>
    * @return Pair @see
    */
  @throws(classOf[ParseException])
  def suffix(log: Map[String, Any]): Date = {
    val sf = if (log.isEmpty) {
      new Date()
    } else {
      log.get(TIMESTAMP) match {
        case Some(timestamp: String) =>
          try {
            makeIndexSuffix(log)
          }
          catch {
            case e: ParseException =>
              parse(timestamp)
          }
        case Some(date: Date) =>
          date
        case _ =>
          (log.get(DATE), log.get(TIME)) match {
            case (Some(date), Some(time)) =>
              parse(date + "T" + time + ".000Z")
            case _ =>
              try {
                makeIndexSuffix(log)
              }
              catch {
                case e: ParseException =>
                  new Date()
              }
          }
      }
    }

    sf
  }

  private def groks(value: String): Option[Map[String, Any]] = {
    val matched: Match = grok.`match`(value)
    matched.captures(true)
    val map = matched.toMap
    if (map.size == 0) {
      None
    }
    else {
      Some(map.toMap)
    }
  }

  @throws(classOf[ParseException])
  private def parse(timestamp: String): Date = {
    if (timestamp.matches("\\d+")) {
      val time: Long = timestamp.toLong


      new Date(time)


    } else if (timestamp.matches("\\d+\\.\\d+")) {
      var time: Double = timestamp.toDouble
      time = time * 1000
      new Date(time.longValue)

    } else {
      groks(timestamp) match {
        case None =>
          new Date()
        case Some(time) =>
          try {
            makeIndexSuffix(time)
          }
          catch {
            case e: ParseException =>
              e.printStackTrace()
              new Date()
          }
      }
    }

  }


}

*/
