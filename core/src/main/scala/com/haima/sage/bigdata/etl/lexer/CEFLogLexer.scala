package com.haima.sage.bigdata.etl.lexer

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common._
import com.haima.sage.bigdata.etl.common.exception.LogParserException
import com.haima.sage.bigdata.etl.common.model.{CefParser, RichMap}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.HashMap

object CEFLogLexer {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[CEFLogLexer])

  case class Lexer(parser: CefParser) extends base.Lexer[String, RichMap] {

    private val separate: String = "\\|"
    private val separateWithSpace: String = " \\| "
    private val cef: Int = "CEF:".length
    private val fields: Array[String] = "version|device_vendor|device_product|device_version|signature_id|name|severity|extension".split("\\|")

    /**
      *
      * CEF:version|device_vendor|device_product|device_version|signature_id|name|severity|a=3,b=4,c=5
      *
      *
      *
      * check each value of the values is end with escaped char: '\',then merge with the next one
      *
      * @param values from
      * @param separate
      * @param length
      * @param trim   need esc space
      * @return to
      */
    private def checkEsc(values: Array[String], separate: String, length: Int, trim: Boolean): Array[String] = {
      var j: Int = 0
      var value: String = null

      values.indices.foreach { case i =>
        if ((i + j) < values.length && values(i + j) != null) {
          if (trim) value = values(i + j).trim
          else value = values(i + j)
          while ((i + j + 1) < values.length && value.endsWith("\\")) {
            j += 1
            if (trim) value += (separate + values(i + j).trim)
            else value += (separate + values(i + j))
          }
        }
        else {
          value = ""
        }
        values(i) = value
      }
      values(length - 1) = values.slice(length - 1, values.length).mkString(separate)
      val len: Int = if ((values.length - j) >= length) length else values.length - j
      values.slice(0, len)
    }

    /**
      * from second value, check each value of the values is contain char: '=' and not contain char: '\=',then merge this to  the before one
      *
      * @param values
      * @param separate
      * @return
      */
    private def checkEquals(values: Array[String], separate: String): Array[String] = {
      var j: Int = 0
      var value: String = null;
      {
        for (i <- values.indices) {
          if ((i + j) < values.length && values(i + j) != null) {
            value = values(i + j)
            while ((i + j + 1) < values.length && !(values(i + j + 1).contains("=") && !values(i + j + 1).contains("\\="))) {
              j += 1
              value += (separate + values(i + j))
            }
          }
          else {
            value = ""
          }
          values(i) = value
        }
      }
      values.slice(0, values.length - j)
    }

    def setTemplet(name: String) {
    }

    def parse(line: String): RichMap = {
      var log: Map[String, Any] = new HashMap[String, AnyRef]
      var values: Array[String] = line.replaceAll(separate, separateWithSpace).split(separate)
      // CEFLogLexer.logger.debug("fields:length{},{}\nvalues:length{},{}", fields.length, fields, values.length, values)
      if (values.length >= fields.length && {
        values = checkEsc(values, separate, fields.length, trim = true)
        values.length == 8
      }) {
        log = log + ((fields(0), values(0).trim.substring(cef)))
        for (i <- 1 to 6 if values(i) != null) {
          if (values(i) != null) {
            val value: String = values(i).trim
            if (!("" == value)) {
              log = log + ((fields(i), value))
            }
          }
        }
        if (values(7) != null) {
          var f7s: Array[String] = values(7).split(" ")
          f7s = checkEsc(f7s, " ", f7s.length, trim = false)
          f7s = checkEquals(f7s, " ")
          f7s.foreach { kv =>
            val kvs: Array[String] = kv.split("=", 2)

            if (kvs.length == 2 && kvs(0) != null && !("" == kvs(0)) && kvs(1) != null && !("" == kvs(1))) {
              log = log + ((kvs(0), kvs(1)))
            }
          }
        }
        log
      } else {
        throw new LogParserException("cef parser:your set fields.length:" + fields.length + ",not match the values.length:" + values.length + "that you want parser")

      }

    }
  }

  def apply(parser: CefParser): Lexer = Lexer(parser)
}

class CEFLogLexer(override val parser: CefParser, override val writers: List[ActorRef], override val report: MeterReport) extends EventLexer {
  val lexer = CEFLogLexer(parser)


}