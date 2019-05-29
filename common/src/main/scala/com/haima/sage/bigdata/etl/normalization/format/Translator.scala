package com.haima.sage.bigdata.etl.normalization.format

import java.math.BigDecimal
import java.util.regex.Pattern
import java.util.zip.DataFormatException
import java.util.{Date, Locale}

import com.haima.sage.bigdata.etl.normalization.ListType
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.commons.lang3.time.{DateParser, FastDateFormat}


/**
  * 类型转换类
  * Created by zhhuiyan on 16/8/22.
  */

trait Translator[T <: Any] {
  def parse(i: Any): T
}

object Translator {
  def apply(_type: String, format: String, toJson: Any => String): Translator[_] = {
    _type match {
      case "idcard" =>
        IDCardTranslator()
      case "phone" =>
        PhoneTranslator()
      case "ip" =>
        IPTranslator()
      case "time" | "date" | "datetime" =>
        format match {
          case null | "" =>
            DefaultTranslator(_type, toJson)
          case f =>
            LocalDateTranslator(f)
        }
      case ListType(_t) =>
        ListTranslator(_t, format, toJson)
      case _ =>
        DefaultTranslator(_type, toJson)
    }

  }
}

/*
* 默认不转换
* */
case class DefaultTranslator(_type: String, toJson: Any => String) extends Translator[Any] {

  private lazy val number: Pattern = Pattern.compile("[-+]?\\d+?")
  private lazy val realP: Pattern = Pattern.compile("[-+]?\\d+(\\.\\d+)?")
  private lazy val scientificNotation: Pattern = Pattern.compile("^(([-+]?\\d+.?\\d*)[Ee]{1}(-?\\d+))$")

  /**
    * 转换字符串到对应的类型
    *
    * @param str    原始字符串
    * @param isReal 转换后是否是实数类型
    * @param f1     默认格式转换方法
    * @param f2     当转换后是非实数,从实数转换成对应类型的方法(四舍五入的方式)
    * @param f3     科学计算法转换方式
    * @tparam T 返回的数据类型
    * @return 返回值T
    */
  private final def convert[T](str: String, isReal: Boolean = true)(f1: String => T, f2: Long => T, f3: BigDecimal => T): T = {
    val real = if (str.startsWith(".")) {
      "0" + str
    } else {
      str
    }
    if (isReal) {

    }


    if (number.matcher(real).matches())
      f1(real)
    else if (isReal && realP.matcher(real).matches())
      f1(real)
    else if (!isReal && realP.matcher(real).matches())
      f2(Math.round(real.toDouble))
    else if (scientificNotation.matcher(real).matches()) {
      f3(new BigDecimal(real))

    } else {
      throw new NumberFormatException(s"your set value[$str] can`t convert to ${_type} ")
    }
  }


  private lazy val p: Any => Any = _type.toLowerCase match {

    case "integer" => {
      case i: Int =>
        i
      case value =>
        convert[Int](value.toString.trim, isReal = false)(_.toInt, _.toInt, _.intValue())
    }

    case "long" => {
      case i: Long =>
        i
      case value =>
        convert[Long](value.toString.trim, isReal = false)(_.toLong, _.toLong, _.longValue())
    }


    case "short" => {
      case i: Short =>
        i
      case value =>
        convert[Short](value.toString.trim, isReal = false)(_.toShort, _.toShort, _.shortValue())
    }

    case "float" => {
      case i: Float =>
        i
      case value =>
        convert[Float](value.toString.trim)(_.toFloat, _.toFloat, _.floatValue())
    }
    case "double" => {
      case i: Double =>
        i
      case value =>
        convert[Double](value.toString.trim)(_.toDouble, _.toDouble, _.doubleValue())
    }
    case "boolean" => {
      case i: Boolean =>
        i
      case value =>
        value.toString.trim.toLowerCase match {
          case "true" =>
            true
          case "false" =>
            false
          case "1" =>
            true
          case "0" =>
            false
          case _ =>
            value
        }
    }
    case "byte" => {
      case i: Byte =>
        i
      case value =>
        value.toString.toByte
    }
    case "string" => {
      case i: String =>
        i
      case value =>
        toJson(value)
    }


    case _ =>
      v => v


  }


  override def parse(i: Any): Any = p(i)
}

/*
*
* * 时间日期转换
* */
case class LocalDateTranslator(format: String) extends Translator[Date] with Serializable with Logger {
  private final lazy val local = new ThreadLocal[DateParser]() with Serializable {
    protected override def initialValue(): DateParser = {
      FastDateFormat.getInstance(format)
    }
  }
  private final lazy val en = new ThreadLocal[DateParser]() with Serializable {
    protected override def initialValue(): DateParser = {
      FastDateFormat.getInstance(format, Locale.ENGLISH)
    }
  }
  private lazy val number: Pattern = Pattern.compile("\\d+?")
  private lazy val f: Pattern = Pattern.compile(format.replaceAll("[YyMmdHhs]", "\\d"))

  /**
    * 转换成时间的方式,当是时间直接返回时间,是数字时,13位毫秒,10位秒,
    *
    * @param value
    * @return
    */
  override def parse(value: Any): Date = {
    value match {
      case i: Date =>
        i
      case i: Long if i >= 1e18 && i < 1e19 =>

        new Date(i / 1000000)
      case i: Long if i >= 1e15 && i < 1e16 =>

        new Date(i / 1000)

      case i: Long if i >= 1e12 && i < 1e13 =>

        new Date(i)
      //      case i: Long if i >= 1e14 =>
      //
      //        new Date(i / Math.pow(10, Math.log10(i).toLong - 12).toLong)
      case i: Long if i > 1e10 =>

        new Date(i * 1000)
      case i: String if number.matcher(i).matches() =>
        if (i.length >= 13)
          new Date(i.toLong)
        else if (i.length >= 10)
          new Date(i.toLong * 1000)
        else
          throw new DataFormatException(s"your set date[$i] can`t be parse by format[$format] and else not a valid number")
      case i: String if f.matcher(i).matches() =>
        try {
          local.get().parse(i)
        } catch {
          case _: Exception =>
            try {
              en.get().parse(i)
            } catch {
              case e: Exception =>
                throw new DataFormatException(s"your set date[$i] can`t be parse by format[$format],cause:" + e.getCause);
            }
        }
      case i =>
        throw new DataFormatException(s"your set date[$i] not a valid,can`t be parse by format[$format] ");

    }
  }
}