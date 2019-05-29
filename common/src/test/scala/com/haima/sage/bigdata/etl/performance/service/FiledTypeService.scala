package com.haima.sage.bigdata.etl.performance.service

import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap
import org.joda.time.DateTime

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class FiledTypeService(val configs: FiledTypeConfig*) {
  private val services = FiledTypeServiceFactory.factory(configs)

  def valid(): Boolean = services.nonEmpty

  def process(event: RichMap): RichMap = Try {
    if (services.length == 1) {
      services.head.process(event)
    } else {
      val map = new mutable.HashMap[String, Any]()
      map ++= event
      val dst = process(map)
      RichMap(dst.toMap)
    }
  }.getOrElse(event)

  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    var res = event
    services.foreach(service => {
      res = service.process(res)
    })
    res
  }.getOrElse(event)
}

object FiledTypeServiceFactory {
  def factory(configs: Seq[FiledTypeConfig]): Seq[FiledTypeItemService] = Try {
    configs.map(config => {
      config.dataType match {
        case "long" =>
          LongFiledTypeService(config)
        case "int" =>
          IntFiledTypeService(config)
        case "float" =>
          FloatFiledTypeService(config)
        case "double" =>
          DoubleFiledTypeService(config)

        case "date" =>
          DateFiledTypeService(config)
        case _ =>
          DefaultFiledTypeService(config)
      }
    })
  }.getOrElse(Nil)
}

case class FiledTypeConfig(field: String, dataType: String, pattern: String = null, trim: Boolean = true)


abstract class FiledTypeItemService(val config: FiledTypeConfig) {
  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any]

  def process(event: RichMap): RichMap
}

case class IntFiledTypeService(override val config: FiledTypeConfig)
  extends FiledTypeItemService(config) {
  def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(_: Int) =>
        return event
      case Some(x: Number) =>
        event += (config.field -> x.intValue())
        return event
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    val data = value.toInt
    event += (config.field -> data)
    event
  }.getOrElse(event - config.field)

  def process(event: RichMap): RichMap = Try {
    val value: String = event.get(config.field) match {
      case Some(_: Int) =>
        return event
      case Some(x: Number) =>
        return event + (config.field -> x.intValue())
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    val data = value.toInt
    event + (config.field -> data)
  }.getOrElse(event - config.field)
}

case class LongFiledTypeService(override val config: FiledTypeConfig)
  extends FiledTypeItemService(config) {
  override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(_: Long) =>
        return event
      case Some(x: Number) =>
        event += (config.field -> x.longValue())
        return event
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    val data = value.toLong
    event += (config.field -> data)
    event
  }.getOrElse(event - config.field)

  override def process(event: RichMap): RichMap = Try {
    val value: String = event.get(config.field) match {
      case Some(_: Long) =>
        return event
      case Some(x: Number) =>
        return event + (config.field -> x.longValue())
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    val data = value.toLong
    event + (config.field -> data)
  }.getOrElse(event - config.field)
}

case class FloatFiledTypeService(override val config: FiledTypeConfig)
  extends FiledTypeItemService(config) {
  override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(_: Float) =>
        return event
      case Some(x: Number) =>
        event += (config.field -> x.floatValue())
        return event
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    val data = value.toFloat
    event += (config.field -> data)
    event
  }.getOrElse(event - config.field)

  override def process(event: RichMap): RichMap = Try {
    val value: String = event.get(config.field) match {
      case Some(_: Float) =>
        return event
      case Some(x: Number) =>
        return event + (config.field -> x.floatValue())
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    val data = value.toFloat
    event + (config.field -> data)
  }.getOrElse(event - config.field)
}

case class DoubleFiledTypeService(override val config: FiledTypeConfig)
  extends FiledTypeItemService(config) {
  override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(_: Double) =>
        return event
      case Some(x: Number) =>
        event += (config.field -> x.doubleValue())
        return event
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    val data = value.toDouble
    event += (config.field -> data)
    event
  }.getOrElse(event - config.field)

  override def process(event: RichMap): RichMap = Try {
    val value: String = event.get(config.field) match {
      case Some(_: Double) =>
        return event
      case Some(x: Number) =>
        return event + (config.field -> x.doubleValue())
      case Some(x: Any) =>
        x.toString
      case None =>
        return event
    }
    val data = value.toDouble
    event + (config.field -> data)
  }.getOrElse(event - config.field)
}


case class DateFiledTypeService(override val config: FiledTypeConfig)
  extends FiledTypeItemService(config) {
  val sdf = new SimpleDateFormat(config.pattern)

  override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
    val value: String = event.get(config.field) match {
      case Some(x: Long) =>
        x.toString.length match {
          case 13 =>
            return event + (config.field -> new Date(x))
          case 10 =>
            return event + (config.field -> new Date(x * 1000))
          case _ =>
            return event
        }
      case Some(_: Date) =>
        return event
      case Some(_: DateTime) =>
        return event
      case Some(x: Any) =>
        if (config.trim) {
          x.toString.trim
        } else {
          x.toString
        }
      case None =>
        return event
    }

    val date = sdf.parse(value)

    event += (config.field -> date)
    event
  } match {
    case Success(value) =>
      value
    case Failure(exception) =>
      event - config.field
  }

  override def process(event: RichMap): RichMap = Try {
    val value: String = event.get(config.field) match {
      case Some(x: Long) =>
        x.toString.length match {
          case 13 =>
            return event + (config.field -> new Date(x))
          case 10 =>
            return event + (config.field -> new Date(x * 1000))
          case _ =>
            return event
        }
      case Some(_: Date) =>
        return event
      case Some(_: DateTime) =>
        return event
      case Some(x: Any) =>
        if (config.trim) {
          x.toString.trim
        } else {
          x.toString
        }
      case None =>
        return event
    }
    val date = sdf.parse(value)

    event + (config.field -> date)
  } match {
    case Success(value) =>
      value
    case Failure(exception) =>
      event - config.field
  }
}

case class DefaultFiledTypeService(override val config: FiledTypeConfig)
  extends FiledTypeItemService(config) {
  override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = event

  override def process(event: RichMap): RichMap = event
}