package com.haima.sage.bigdata.etl.server.hander

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Title: 
  * Description: 
  *
  * @author lianxy
  * date 2018/01/22
  */
object DateUtils {
    val SECOND_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd")
//    val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
    val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")

    /**
      * 判断一个时间是否在另一个时间之前
      *
      * @param time1 第一个时间
      * @param time2 第二个时间
      * @return 判断结果
      */
    def before(time1: String, time2: String, formatter: String): Boolean = {
        val dateTime1 = SECOND_FORMATTER.parse(time1)
        val dateTime2 = SECOND_FORMATTER.parse(time2)
        if(dateTime1.before(dateTime2)) {
            true
        }

        false
    }

    /**
      * 判断一个时间是否在另一个时间之后
      *
      * @param time1 第一个时间
      * @param time2 第二个时间
      * @return 判断结果
      */
    def after(time1: String, time2: String, formatter: String): Boolean = {
        val dateTime1 = SECOND_FORMATTER.parse(time1)
        val dateTime2 = SECOND_FORMATTER.parse(time2)
        if(dateTime1.after(dateTime2)) {
            true
        }

        false
    }

    /**
      * 计算时间差值（单位为秒）
      *
      * @param time1 时间1
      * @param time2 时间2
      * @return 差值
      */
    def minus(time1: String,time2: String, formatter: String) {
        val datetime1 = SECOND_FORMATTER.parse(time1)
        val datetime2 = SECOND_FORMATTER.parse(time2)
        val millisecond = datetime1.getTime() - datetime2.getTime()
        Integer.valueOf(String.valueOf(millisecond / 1000))
    }

    /**
      * 获取年月日和小时
      *
      * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
      * @return 结果（yyyy-MM-dd_HH）
      */
    def getDateHour(datetime: String): String = {
        val date = datetime.split(" ")(0)
        val hourMinuteSecond = datetime.split(" ")(1)
        val hour = hourMinuteSecond.split(":")(0)
        date + "_" + hour
    }

    /**
      * 获取当天日期（yyyy-MM-dd）
      *
      * @return 当天日期
      */
    def getTodayDate(formatter: String): String = {
        DATE_FORMATTER.format(new Date())
    }

    /**
      * 获取昨天的日期（yyyy-MM-dd）
      *
      * @return 昨天的日期
      */
    def getYesterdayDate(formatter: String): String = {
        val cal = Calendar.getInstance()
        cal.setTime(new Date())
        cal.add(Calendar.DAY_OF_YEAR, -1)
        val date = cal.getTime()
        DATE_FORMATTER.format(date)
    }

    /**
      * 格式化日期（yyyy-MM-dd）
      *
      * @param date Date对象
      * @return 格式化后的日期
      */
    def formatDate(date: Date, formatter: String): String = {
        if(formatter != null){
            new SimpleDateFormat(formatter).format(date)
        } else {
            DATE_FORMATTER.format(date)
        }
    }

    /**
      * 格式化时间（yyyy-MM-dd HH:mm:ss）
      *
      * @param date Date对象
      * @return 格式化后的时间
      */
    def formatTime(date: Date, formatter: String): String = {
        if(formatter != null){
            new SimpleDateFormat(formatter).format(date)
        } else {
            SECOND_FORMATTER.format(date)
        }
    }

    /**
      * 解析时间字符串
      *
      * @param time 时间字符串
      * @return Date
      */
    def parseTime(time: String, formatter: String): Date = {
        if(formatter != null){
            new SimpleDateFormat(formatter).parse(time)
        } else {
            SECOND_FORMATTER.parse(time)
        }
    }

    /**
      * 格式化日期key
      *
      * @param date
      * @return
      */
    def formatDateKey(date: Date, formatter: String) {
        DATEKEY_FORMAT.format(date)
    }

    /**
      * 格式化日期key
      *
      * @param datekey
      * @return
      */
    def parseDateKey(datekey: String, formatter: String) {
        DATEKEY_FORMAT.parse(datekey)
    }

    /**
      * 格式化时间，保留到分钟级别
      * yyyyMMddHHmm
      *
      * @param date
      * @return
      */
    def formatTimeMinute(date: Date) {
        val sdf = new SimpleDateFormat("yyyyMMddHHmm")
        sdf.format(date)
    }

}
