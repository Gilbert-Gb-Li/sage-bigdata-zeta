package com.haima.sage.bigdata.etl.utils

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar

/**
 * Created by zhhuiyan on 2015/2/26.
 */
object TimeUtils {
  private final val default = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
  }
  def currentDate: java.util.Date = Calendar.getInstance().getTime()

  def defaultFormat(date: java.util.Date): String = default.get().format(date)

  def defaultParse(dateStr: String): java.util.Date = default.get().parse(dateStr)

  /**
   * 获取指定日期指定分钟之前的日期
   * @param date
   * @param minute
   * @return
   */
  def minuteBefore(date: java.util.Date, minute: Int): java.util.Date = {
    new java.util.Date(date.getTime - minute * 60 * 1000)
  }

  /**
   * 获取指定日期指定分钟之前的日期
   * @param dateStr(yyyy-MM-dd HH:mm:ss)
   * @param minute
   * @return
   */
  def minuteBefore(dateStr: String, minute: Int): String = {
    defaultFormat(new java.util.Date(defaultParse(dateStr).getTime - minute * 60 * 1000))
  }

  /**
   * 获取指定日期指定分钟之后的日期
   * @param date
   * @param minute
   * @return
   */
  def minuteAfter(date: java.util.Date, minute: Int): java.util.Date = {
    new java.util.Date(date.getTime + minute * 60 * 1000)
  }

  /**
   * 获取指定日期指定分钟之后的日期
   * @param dateStr(yyyy-MM-dd HH:mm:ss)
   * @param minute
   * @return
   */
  def minuteAfter(dateStr: String, minute: Int): String = {
    defaultFormat(new java.util.Date(defaultParse(dateStr).getTime + minute * 60 * 1000))
  }

  /**
   * 时间比较器
   * @param date1
   * @param date2
   * @return 如果小于0，date1 早于 date2；如果大于0，date2 早于 date1；
   */
  def dateCompare(date1: java.util.Date, date2: java.util.Date): Long = {
    date1.getTime - date2.getTime
  }
}
