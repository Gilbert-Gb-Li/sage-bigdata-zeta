package com.haima.sage.bigdata.etl.normalization

import java.util

import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.common.model.{MapUtils, Parser, RichMap}
import com.haima.sage.bigdata.etl.normalization.format._
import org.slf4j.LoggerFactory

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


/**
  * Created by zhhuiyan on 15/1/29.
  */


trait Normalizer extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[Normalizer].getName)

  def parser: Parser[MapRule]

  def toJson(value: Any): String


  private def metaFilter(t: (String, String, String)): Boolean = {
    t._1 != null && t._1.trim != "" && t._1.trim != "string"
  }

  private def metaMap(threeTuple: (String, String, String)): (String, (Translator[_], (java.util.Map[String, Any], String) => Unit)) = {
    val translator: Translator[_] = Translator(threeTuple._2, threeTuple._3, toJson)

    val func: (util.Map[String, Any], String) => Unit = translate(translator)
    (threeTuple._1, (translator, func))
  }

  private lazy val mates: Map[String, (Translator[_], (java.util.Map[String, Any], String) => Unit)] = parser.metadata.getOrElse(List()).filter(metaFilter).map(metaMap).toMap

  def isDot: Boolean = mates.exists(_._1.contains("."))


  private final def translate(translator: Translator[_])(data: java.util.Map[String, Any], key: String): Unit = {
    Option(data.get(key)) match {
      case Some(value) =>
        Try(translator.parse(value)) match {
          case Success(d) =>
            data.put(key, d)
          case Failure(exception) =>
            logger.warn(s"""use $translator normalizing ${(key, value)},class:${value.getClass}  failure :$exception""")
            data.put("error", s"""use $translator normalizing ${(key, value)},class:${value.getClass}  failure :$exception""")
        }
      case _ =>
    }

  }

  private final def translateOne(event: java.util.Map[String, Any], one: (String, (Translator[_], (java.util.Map[String, Any], String) => Unit))): java.util.Map[String, Any] = {
    one match {
      case (key, translator) =>
        MapUtils.recursionJava(event, key)(translator._2)


    }
  }

  private final def translateScalaOne(event: RichMap, one: (String, (Translator[_], (java.util.Map[String, Any], String) => Unit))): RichMap = {
    one match {
      case (key, translator) =>
        event.get(key) match {
          case Some(value) if value != null =>
            Try(translator._1.parse(value)) match {
              case Success(d) =>
                event + (key -> d)
              case Failure(exception) =>
                logger.warn(s"""use $translator normalizing ${(key, value)},class:${value.getClass}  failure :$exception""")
                event + ("error" -> s"""use $translator normalizing ${(key, value)},class:${value.getClass}  failure :$exception""")
            }
          case _ =>
            event
        }


    }
  }

  private lazy final val normalize0: RichMap => RichMap = if (isDot) {
    event =>
      val rts = mates.foldLeft(MapUtils.toJava(event))(translateOne)
      import scala.collection.JavaConversions._
      RichMap(rts.toMap)
  } else {
    event => {
      mates.foldLeft(event)(translateScalaOne)
    }
  }

  def normalize(event: RichMap): RichMap = {
    normalize0(event)
  }
}

object ListType {

  final val regex: Regex ="""list\[(.*)]""".r

  def unapply(str: String): Option[String] = {
    regex.unapplySeq(str) match {
      case Some(user :: Nil) => Some(user)
      case _ =>
        None
    }
  }

}