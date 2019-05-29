package com.haima.sage.bigdata.etl.writer

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.dataformat.xml.{JacksonXmlModule, XmlMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.haima.sage.bigdata.etl.common.model.writer._
import com.haima.sage.bigdata.etl.utils.{Logger, Mapper}


/**
  * Created by zhhuiyan on 2016/10/19.
  */

object Formatter {
  def apply(contentType: Option[ContentType]): MapFormat = contentType match {
    case Some(Json(encoding)) =>
      MapToJsonFormat(encoding)
    case Some(Xml(encoding)) =>
      MapToXmlFormat(encoding)
    case Some(del: Delimit) =>
      MapToDelFormat(del)
    case Some(syslog: Syslog) =>
      SyslogFormat(syslog)
    case Some(delKM: DelimitWithKeyMap) =>
      MapToDelKMFormat(delKM)
    case _ =>
      MapToJsonFormat()
  }
}


trait MapFormat extends Serializable with Logger {
  def string(data: Map[String, Any]): String

  def bytes(data: Map[String, Any]): Array[Byte]

  def real(data: Map[String, Any]): Any =
    if (data.size == 1 && data.contains("raw")) {
      data.getOrElse("raw", "")
    } else {
      data
    }
}

abstract class JackSonMapFormat(encoding: Option[String] = None) extends MapFormat {

  private[writer] val mapper: ObjectMapper

  override def string(data: Map[String, Any]): String = {

    val real_data = real(data)

    real_data match {
      case _data: String =>
        encoding match {
          case Some(enc: String) if enc.trim != "" =>
            new String(_data.getBytes(), enc).trim
          case _ =>
            _data.trim
        }
      case _data =>
        encoding match {
          case Some(enc: String) if enc.trim != "" =>
            new String(mapper.writeValueAsBytes(_data), enc).trim
          case _ =>
            mapper.writeValueAsString(_data).trim

        }
    }


  }

  override def bytes(data: Map[String, Any]): Array[Byte] = {

    val real_data = real(data)

    real_data match {
      case _data: String =>
        encoding match {
          case Some(enc: String) if enc.trim != "" =>
            _data.trim.getBytes(enc)
          case _ =>
            _data.trim.getBytes
        }
      case _data =>

        encoding match {
          case Some(enc: String) if enc.trim != "" =>
            new String(mapper.writeValueAsBytes(_data), enc).getBytes
          case _ =>
            mapper.writeValueAsBytes(_data)
        }

    }


  }
}

abstract class StringMapFormat(encoding: Option[String] = None) extends MapFormat {
  private[writer] final val format = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S Z")
    }
  }


  private[writer] def line(data: Map[String, Any]): String

  override def string(data: Map[String, Any]): String = {

    encoding match {
      case Some(enc: String) if enc.trim != "" =>
        new String(line(data).getBytes, enc).trim
      case _ =>
        line(data)
    }

  }

  override def bytes(data: Map[String, Any]): Array[Byte] = {
    encoding match {
      case Some(enc: String) if enc.trim != "" =>
        new String(line(data).getBytes, enc).getBytes
      case _ =>
        line(data).getBytes
    }


  }


}

case class MapToJsonFormat(encoding: Option[String] = None) extends JackSonMapFormat(encoding) with Mapper {


}

case class MapToXmlFormat(encoding: Option[String] = None) extends JackSonMapFormat(encoding) {

  val module = new JacksonXmlModule()
  module.setDefaultUseWrapper(false)
  private[writer] override val mapper: ObjectMapper = new XmlMapper(module) with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setVisibility(PropertyAccessor.FIELD, Visibility.PUBLIC_ONLY)
  mapper.registerModule(DefaultScalaModule).setSerializationInclusion(Include.NON_NULL)

  mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
  mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false)

  override def string(data: Map[String, Any]): String = {

    val real_data = real(data)

    real_data match {
      case _data: String =>
        encoding match {
          case Some(enc: String) if enc.trim != "" =>
            new String(_data.getBytes(), enc)
          case _ =>
            _data
        }
      case _data: Map[String, Any]@unchecked =>
        val set = _data.keySet
        val it = set.iterator
        val sb = new StringBuffer("")


        while (it.hasNext) {
          val key = it.next
          val value = _data.get(key) match {
            case Some(v) => v
            case None =>
              ""
          }
          sb.append("<").append(key).append(">")
          sb.append(value)
          sb.append("</").append(key).append(">")
        }
        encoding match {
          case Some(enc: String) if enc.trim != "" =>
            new String(sb.toString.getBytes, enc)
          case _ =>
            sb.toString

        }
      case _ =>
        ""
    }
  }

}

case class SyslogFormat(syslog: Syslog = Syslog()) extends MapFormat {

  import java.net.InetAddress

  private final lazy val format = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat("MMM dd HH:mm:ss")
    }
  }
  private[writer] final val dateTimeFormat = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
  }
  private final lazy val mapper = Formatter(syslog.contentType)

  override def string(data: Map[String, Any]): String = {

    //主机
    val host: String = syslog.syslogFormat.hostname match {
      case Some(field) =>
        if (field.trim.startsWith("$")) {
          data.get(field.substring(field.indexOf("$") + 1)) match {
            case Some(v) => v.toString.trim
            case None => InetAddress.getLocalHost.getHostAddress //需要确认没有字段是异常还是LocalHost
          }
        } else
          field.trim
      case None => InetAddress.getLocalHost.getHostAddress
    }
    //tag
    val tag: String = syslog.syslogFormat.tag match {
      case Some(field) =>
        if (field.trim.startsWith("$")) {
          data.get(field.substring(field.indexOf("$") + 1)) match {
            case Some(v) => v.toString.trim
            case None => "" //需要确认没有字段是异常还是
          }
        } else
          field.trim
      case None => ""
    }
    //时间
    val date: Date = syslog.syslogFormat.timestampType match {
      case Some("DATA") =>
        syslog.syslogFormat.timestamp match {
          case Some(field) =>
            data.get(field) match {
              case Some(d: String) =>
                dateTimeFormat.get().parse(d)

              case _ =>
                new Date()
            }
          case None =>
            new Date()
        }
      case Some("VAL") =>
        syslog.syslogFormat.timestamp match {
          case Some(field) =>
            dateTimeFormat.get().parse(field)
          case None =>
            new Date()
        }
      case _ => new Date()
    }

    //优先级  PRI = Facility * 8 + Level。
    val priority: String = syslog.syslogFormat.priorityType match {
      case Some("DATA") =>
        syslog.syslogFormat.priority match {
          case Some(field) =>
            data.get(field) match {
              case Some(v) => v.toString
              case None => "0"
            }
          case None => "0"
        }
      case Some("VAL") =>
        if (syslog.syslogFormat.facility.nonEmpty && syslog.syslogFormat.level.nonEmpty)
          (syslog.syslogFormat.facility.get.toInt * 8 + syslog.syslogFormat.level.get.toInt).toString
        else
          "0"
      case _ =>
        "0"
    }

    if ("".equals(tag))
      s"<$priority>${format.get().format(date)} $host ${mapper.string(data).trim}"
    else
      s"<$priority>${format.get().format(date)} $host $tag ${mapper.string(data).trim}" //SyslogMessage(Header(Priority(1),"",""), mapper.string(data)).toString
  }

  override def bytes(data: Map[String, Any]): Array[Byte] = string(data).getBytes
}

case class MapToDelFormat(delimit: Delimit) extends StringMapFormat(delimit.encoding) {


  var fields: Array[String] = delimit.fields match {
    case Some(fs: String) =>
      fs.split(",")
    case _ =>
      Array()
  }
  val del: String = delimit.delimit.getOrElse(",")

  private[writer] override def line(data: Map[String, Any]): String = {


    if (data.size == 1 && data.contains("raw")) {
      data.getOrElse("raw", "").toString.trim
    } else {
      if (fields.length <= 0) {
        fields = data.keys.toArray
      }
      fields.map {
        field =>
          data.get(field) match {
            case Some(d: String) if d.contains(del) =>
              s"""'$d'"""
            case Some(d: Date) =>
              s"""'${format.get().format(d)}'"""
            case Some(d) =>
              d
            case _ =>
              ""

          }
      }.mkString(delimit.delimit.getOrElse(",")).trim
    }

  }

}

case class MapToDelKMFormat(delimit: DelimitWithKeyMap) extends StringMapFormat(delimit.encoding) {
  val tab: String = delimit.tab match {
    case Some(d: String) if d == "" =>
      "="
    case Some(d: String) =>
      d
    case _ =>
      "="
  }
  val del: String = delimit.delimit match {
    case Some(d: String) if d == "" =>
      ","
    case Some(d: String) =>
      d
    case _ =>
      ","
  }

  private[writer] override def line(data: Map[String, Any]): String =
    if (data.size == 1 && data.contains("raw")) {
      data.getOrElse("raw", "").toString.trim
    } else {
      data.filter(_._2 != null).map {
        case (key, v) =>
          val value = v match {
            case d: String if d.contains(del) || d.contains(tab) =>
              s"""'$d'"""
            case d: Date =>
              s"""'${format.get().format(d)}'"""
            case d =>
              d
          }
          s"$key$tab$value"
      }.mkString(del).trim
    }


}