package com.haima.sage.bigdata.etl.lexer


import java.util

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{DelimitWithKeyMap, RichMap}
import com.haima.sage.bigdata.etl.common.{QuoteString, _}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.slf4j.{Logger, LoggerFactory}


class DelimiterWithKeyMapLexer(override val parser: DelimitWithKeyMap, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {
  val lexer = DelimiterWithKeyMapLexer(parser)

}

object DelimiterWithKeyMapLexer {

  case class Lexer(parser: DelimitWithKeyMap) extends base.Lexer[String, RichMap] {
    private val separate: String = parser.delimit.getOrElse(",")
    private val tab: String = parser.tab.getOrElse("=")
    private val singer = '''
    private val doubler = '"'
    private val separateWithSpace: String = {
      separate match {
        case " " =>
          "\t" + separate + "\t"
        case _ =>
          " " + separate + " "
      }

    }

    private def get(implicit value: String): String = {
      value match {
        case "" =>
          null
        case QuoteString(_, "null", _) =>
          null
        case QuoteString(_, "N/A", _) =>
          null
        case QuoteString(_, v, _) =>
          v
        case _ =>
          value
      }
    }

    def quote(implicit value: String): String = {
      value match {
        case QuoteString(_, v, _) =>
          v
        case _ =>
          value
      }
    }

    /*是否在引号中*/
    private def inQuote(head: Array[Char], ch: Char): Boolean = {
      if (head.length == 0)
        false
      else {
        var i = 0
        var in = false
        while (i < head.length) {
          if (head(i) == ch) {
            in = !in
            i += 1
          } else if (head(i) == '\\') {
            i += 2
          } else {
            i += 1
          }

        }
        in
      }

    }

    def parse(event: String): RichMap = {
      var parent = ""
      var last_key = ""
      val record: util.Map[String, Any] = new util.HashMap[String, Any]()


      var tail = event.trim
      var i = tail.indexOf(separate)
      while (i != -1) {
        val line = tail.substring(0, i)
        tail = tail.substring(i + separate.length)
        val j = line.indexOf(tab)
        if (j != -1) {
          val key = line.substring(0, j)
          var value = line.substring(j + tab.length)

          if (inQuote(value.toCharArray, singer)) {
            i = tail.indexOf(singer + separate)
            if (i != -1) {
              var v2 = tail.substring(0, i + 1)
              value = value + separate + v2
              tail = tail.substring(i + 1)

              while (!inQuote(v2.toCharArray, singer) && i != -1) {
                i = tail.indexOf(singer + separate)
                if (i != -1) {
                  v2 = tail.substring(0, i + 1)
                  value = value + separate + v2
                  tail = tail.substring(i + 1)
                } else {
                  value = value + separate + tail
                  tail = ""
                }
              }
            } else {
              value = value + separate + tail
              tail = ""

            }
            i = 0
          } else if (inQuote(value.toCharArray, doubler)) {
            i = tail.indexOf(doubler + separate)
            if (i != -1) {
              var v2 = tail.substring(0, i + 1)
              value = value + separate + v2
              tail = tail.substring(i + 1)

              while (!inQuote(v2.toCharArray, doubler) && i != -1) {
                i = tail.indexOf(doubler + separate)
                if (i != -1) {
                  v2 = tail.substring(0, i + 1)
                  value = value + separate + v2
                  tail = tail.substring(i + 1)
                } else {
                  value = value + separate + tail
                  tail = ""
                }
              }

            } else {
              value = value + separate + tail
              tail = ""
            }
            i = 0
          }
          last_key = if (parent == "") {
            key

          } else {
            val key2 = parent + "_" + key

            parent = ""
            key2
          }
          record.put(last_key.trim, get(value))
        } else {
          if (parent == "") {
            parent = line
          } else {
            parent = parent + "_" + line
          }

        }
        i = tail.indexOf(separate)
      }
      tail.indexOf(tab) match {

        case -1 =>
          if (tail.length > 0)
            record.put(last_key.trim, record.get(last_key) + separate + tail)
        case num =>
          val key = tail.substring(0, num)
          val value = tail.substring(num + tab.length)
          last_key = if (parent == "") {
            key

          } else {
            val key2 = parent + "_" + key
            key2
          }
          record.put(last_key.trim, get(value))

      }

      import scala.collection.JavaConversions._
      record.toMap
    }


    def parseO(event: String): Map[String, Any] = {

      // object name 缓存，(value, 使用次数标识符)
      var objectIndex = ("", 0)
      // 解析字段存储map
      val log_map = new scala.collection.mutable.HashMap[String, String]()
      // value包含起始结束符的 key ，例如 key="abc 123" ,quote_key=key
      var inQuoteKey = ""
      val values: Array[String] = event.replaceAll(separate, separateWithSpace).split(separate)
      values.foreach {
        v =>
          val v_trim = v.trim
          //不包含tab
          if (!v_trim.contains(tab)) {
            // 切 是在引号中
            if (inQuoteKey != "") {
              if (log_map.contains(inQuoteKey)) {
                log_map.+=(inQuoteKey -> quote(log_map(inQuoteKey) + separate + v_trim))
              }

              if (v_trim.endsWith("\"") || v_trim.endsWith("'"))
                inQuoteKey = ""
            } else {
              // 不在引号中 则认为是一个对象定义开始
              objectIndex = (v_trim, 0)
            }
          } else {
            val vs = v_trim.split(tab, 2)
            val key = vs(0).trim
            val value = get(vs(1).trim)

            if (value != null) {
              if (value.startsWith("\"") || value.startsWith("'")) {
                if (log_map.contains(key)) {
                  if (objectIndex._1 != "") {
                    inQuoteKey = objectIndex._1 + "." + key
                  } else {
                    inQuoteKey = key
                  }
                }

                else
                  inQuoteKey = key
              }
              // objectIndex 当首次使用0，或者 log_map中包含这个key时
              val _key: String = if (objectIndex._1 != "" && (objectIndex._2 == 0 || log_map.contains(key))) {
                objectIndex = (objectIndex._1, objectIndex._2 + 1)
                objectIndex._1 + "." + key
              } else {
                key
              }
              if (_key.length > 50) {
                log_map.+=("error" -> s"your key is length then 50 maybe parser is error :${_key}->${quote(value)}")
              } else {
                log_map.+=(_key -> quote(value))
              }

            }

          }
      }
      log_map.toMap[String, AnyRef]
    }
  }

  def apply(parser: DelimitWithKeyMap): Lexer = {
    Lexer(parser)
  }

  val logger: Logger = LoggerFactory.getLogger(classOf[RegexLexer])


}