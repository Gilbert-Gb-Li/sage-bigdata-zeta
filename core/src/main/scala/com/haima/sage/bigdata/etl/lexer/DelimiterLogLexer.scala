package com.haima.sage.bigdata.etl.lexer

import java.util

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.exception.LogParserException
import com.haima.sage.bigdata.etl.common.model.{Delimit, RichMap}
import com.haima.sage.bigdata.etl.common.{QuoteString, _}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.slf4j.{Logger, LoggerFactory}


class DelimiterLexer(override val parser: Delimit, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {
  val lexer = DelimiterLexer(parser)
}

object DelimiterLexer {

  case class Lexer(parser: Delimit) extends base.Lexer[String, RichMap] {
    private val delimiter = Delimiter(parser.delimit, parser.fields.length)
    private val fields: Array[String] = parser.fields

    @throws(classOf[LogParserException])
    def parse(event: String): RichMap = {


      val real = delimiter.parse(event)

      def make(values: List[String]) = {
        //   values(fields.length - 1).replaceAll(separateWithSpace, separate)
        fields.zip(values).toMap
      }

      if (fields.length == real.length) {
        make(real)
      } else if (fields.length < real.length) {
        val separate = delimiter.delimit.getOrElse(",")
        val values: List[String] = real.slice(0, fields.length - 1) :+ real.slice(fields.length - 1, real.length).mkString(separate)
        make(values)
      } else {
        throw new LogParserException(s"your set fields.length:${fields.length},not match the values.length:${real.length}that you want parser; data formate and parser rules not match, delimiter: '${parser.delimit.getOrElse(",")}'")
      }
    }
  }

  def apply(parser: Delimit): Lexer = {
    Lexer(parser)
  }


}

case class Delimiter(delimit: Option[String], var length: Int) {

  val logger: Logger = LoggerFactory.getLogger(classOf[RegexLexer])
  private val separate: String = delimit.getOrElse(",")
  val separate_len = separate.length
  /*@tailrec
  private final def next(data: Iterator[String], reg: String, from: String): String = {
    if (data.hasNext) {
      val d = data.next().trim
      if (d.endsWith(reg)) {

        from.concat(separate).concat(d.substring(0, d.length - 1))
      } else {
        next(data, reg, from.concat(separate).concat(d))
      }
    } else {
      from
    }
  }

  private final def contains(value: String, ch: Char) = {


    try {
      value.charAt(0) == ch && value.toCharArray.map {
        case `ch` =>
          1
        case _ => 0
      }.sum % 2 == 1
    } catch {
      case e: Exception =>
        logger.error(s"$value match error :$e")
        throw e
    }

  }*/

  def inQuote(head: Array[Char], ch: Char): Boolean = {
    if (head.length == 0)
      false
    else if (head.length >= 2 && head(0) == ch && head(head.length - 1) == ch) {
      false
    } else {
      head(0) == ch && head.map {
        case `ch` =>
          1
        case _ => 0
      }.sum % 2 == 1
    }

  }

  def parse(event: String): List[String] = {

    val record: util.List[String] = new util.ArrayList[String]
    var tail = event
    var i = tail.indexOf(separate)
    while (i != -1 && record.size() < length - 1) {
      var head = tail.substring(0, i)
      var heads = head.toCharArray
      while (i != -1 && inQuote(heads, '\'')) {
        i = tail.indexOf(separate, i + separate_len)
        if (i != -1) {
          head = tail.substring(0, i)
          heads = head.toCharArray
        }
      }
      while (i != -1 && inQuote(heads, '\"')) {
        i = tail.indexOf(separate, i + separate_len)
        if (i != -1) {
          head = tail.substring(0, i)
          heads = head.toCharArray
        }
      }
      if (i != -1) {
        record.add(head.trim match {
          case "" | "-" => null
          case QuoteString(_, data, _) =>
            data
          case data =>
            data
        })

        tail = tail.substring(i + separate_len)
        i = tail.indexOf(separate)
      }
    }
    record.add(tail.trim match {
      case "-" => null
      case QuoteString(_, data, _) =>
        data
      case data =>
        data
    })
    import scala.collection.JavaConversions._
    record.toList
  }

}