package com.haima.sage.bigdata.etl.common.model.writer

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.{As, Id}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.haima.sage.bigdata.etl.common.model.RichMap


/**
  * Created by zhhuiyan on 2016/10/19.
  */
@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[Json], name = "json"),
  new Type(value = classOf[Xml], name = "xml"),
  new Type(value = classOf[Syslog], name = "syslog"),
  new Type(value = classOf[Delimit], name = "del"),
  new Type(value = classOf[OrcType], name = "orc"),
  new Type(value = classOf[DelimitWithKeyMap], name = "delkv")
))
trait ContentType extends Serializable {
  def encoding: Option[String]
}

case class Json(encoding: Option[String] = None) extends ContentType

case class Xml(encoding: Option[String] = None) extends ContentType

case class Syslog(contentType: Option[ContentType] = Some(Json()), syslogFormat: SyslogOutputFormat = SyslogOutputFormat()) extends ContentType {
  override def encoding: Option[String] = contentType.map(_.encoding.orNull)
}

case class OrcType(fields: Array[(String, Any)]) extends ContentType {
  override def encoding: Option[String] = None

  override def toString: String = s"struct<${fields.map(kv => s"${kv._1}:${kv._2}").mkString(",")}>"
}

case class Delimit(delimit: Option[String] = Some(","), fields: Option[String], encoding: Option[String] = None) extends ContentType

case class DelimitWithKeyMap(delimit: Option[String] = Some(","), tab: Option[String] = Some(","), encoding: Option[String] = None) extends ContentType


/**
  * Created by zhhuiyan on 2016/11/15.
  */
@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "name")
@JsonSubTypes(Array(
  new Type(value = classOf[Ref], name = "ref"),
  new Type(value = classOf[RefDate], name = "date"),
  new Type(value = classOf[RefTruncate], name = "truncate"),
  new Type(value = classOf[RefHashMod], name = "mod"),
  new Type(value = classOf[RefNone], name = "none")))
class NameType extends Serializable {

}

case class Ref(field: String) extends NameType

case class RefNone() extends NameType

case class RefDate(field: String, format: String) extends NameType

case class RefTruncate(field: String, end: Boolean, from: Int, length: Int) extends NameType

case class RefHashMod(field: String, mod: Int) extends NameType


object NameFormatter {
  def apply(pattern: String, nameType: Option[NameType]): NameFormat = {
    nameType match {
      case Some(ref: Ref) =>
        RefFormat(pattern, ref)
      case Some(ref: RefDate) =>
        RefDateFormat(pattern, ref)
      case Some(ref: RefTruncate) =>
        RefTruncateFormat(pattern, ref)
      case Some(ref: RefHashMod) =>
        RefHashModFormat(pattern, ref)
      case _ =>
        RefNoneFormat(pattern)
    }
  }
}


trait NameFormat {
  def pattern: String

  val model: Int = pattern match {
    //需要替换引用值
    case p: String if p.trim.length > 0 =>
      if (p.contains("$")) {
        2
      } else {
        //不需要替换,固定值
        1
      }
    case _ =>
      //其他情况,空字符串
      0
  }

  val format0: RichMap => String

  def format(data: RichMap): String = format0(data)
}

case class RefFormat(override val pattern: String, ref: Ref) extends NameFormat {


  lazy val format0 = if (model == 0) {
    data: RichMap => data.getOrElse(ref.field, "default").toString
  } else if (model == 1) {
    _: RichMap => pattern
  } else {
    data: RichMap =>
      pattern.replace("$", data.getOrElse(ref.field, "default").toString)

  }

}

case class RefNoneFormat(override val pattern: String) extends NameFormat {
  lazy val format0 = if (model == 0) {
    _: RichMap => "default"
  } else if (model == 1) {
    _: RichMap => pattern
  } else {
    _: RichMap => pattern.replace("$", "")

  }


}

case class RefDateFormat(override val pattern: String, ref: RefDate) extends NameFormat {

  private final val FORMAT = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat(
        ref.format match {
          case msg: String if msg.trim.length > 0 =>
            msg

          case _ =>
            "yyyyMMdd"
        })
    }
  }
  lazy val format0 = if (model == 0) {
    data: RichMap => format1(data)
  } else if (model == 1) {
    _: RichMap => pattern
  } else {
    data: RichMap => pattern.replace("$", format1(data))

  }


  def format1(data: RichMap): String = {
    data.get(ref.field) match {
      case Some(data: Long) =>
        FORMAT.get().format(new Date(data))
      case Some(data: Date) =>
        FORMAT.get().format(data)
      case Some(data: BigDecimal) =>
        FORMAT.get().format(new Date(data.longValue()))
      case Some(data: String) if data.matches("\\d+") =>
        FORMAT.get().format(new Date(data.toLong))
      case _ =>
        "default"
    }
  }
}

case class RefTruncateFormat(override val pattern: String, ref: RefTruncate) extends NameFormat {


  override val model: Int = {
    if (ref.length < 1) {
      1
    } else {
      pattern match {
        //需要替换引用值
        case p: String if p.trim.length > 0 =>
          if (p.contains("$")) {
            2
          } else {
            //不需要替换,固定值
            1
          }
        case _ =>
          //其他情况,空字符串
          0
      }
    }


  }
  lazy val format0 = if (model == 0) {
    data: RichMap => format1(data)
  } else if (model == 1) {
    _: RichMap => pattern
  } else {
    data: RichMap => pattern.replace("$", format1(data))

  }

  def format1(data: RichMap): String = {
    def rt: String = data.get(ref.field) match {
      case Some(d) if d != null =>
        val d1 = d.toString.trim


        if (!ref.end && ref.from >= 0 && d1.length >= ref.from + ref.length) {
          d1.substring(ref.from, ref.from + ref.length)
        } else if (!ref.end && ref.from >= 0 && d1.length > ref.from) {
          d1.substring(ref.from, d1.length)
        } else if (ref.from >= 0 && d1.length >= ref.from + ref.length) {
          d1.substring(d1.length - ref.from - ref.length, d1.length - ref.from)
        } else if (ref.from >= 0 && d1.length > ref.from) {
          d1.substring(0, d1.length - ref.from)
        } else {
          "default"
        }
      case _ =>
        "default"
    }

    if (model == 0) {
      rt
    } else if (model == 1) {
      pattern
    } else {
      pattern.replace("$", rt)

    }


  }
}

case class RefHashModFormat(override val pattern: String, ref: RefHashMod) extends NameFormat {
  override val model: Int = {
    if (ref.mod <= 0) {
      1
    } else {
      pattern match {
        //需要替换引用值
        case p: String if p.trim.length > 0 =>
          if (p.contains("$")) {
            2
          } else {
            //不需要替换,固定值
            1
          }
        case _ =>
          //其他情况,空字符串
          0
      }
    }


  }
  lazy val format0 = if (model == 0) {
    data: RichMap => format1(data)
  } else if (model == 1) {
    _: RichMap => pattern
  } else {
    data: RichMap => pattern.replace("$", format1(data))

  }

  def format1(data: RichMap): String = {
    data.get(ref.field) match {
      case Some(d) if d != null =>
        (d.hashCode() % ref.mod).abs.toString
      case _ =>
        "default"
    }
  }
}



