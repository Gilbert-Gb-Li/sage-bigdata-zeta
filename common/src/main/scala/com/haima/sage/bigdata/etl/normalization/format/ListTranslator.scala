package com.haima.sage.bigdata.etl.normalization.format

import com.haima.sage.bigdata.etl.utils.Logger

/**
  * Created by zhhuiyan on 16/8/22.
  */
case class ListTranslator(_type: String, format: String, toJson: (Any) => String) extends Translator[Any] with Serializable with Logger {

  import scala.collection.JavaConversions._

  val translator: Translator[_] = Translator(_type, format, toJson)

  override def parse(value: Any): Any = {
    value match {
      case d: List[Any] =>
        d.map(v => translator.parse(v))
      case d: Array[Any] =>
        d.map(v => translator.parse(v))
      case d: java.util.List[Any@unchecked] =>
        d.map(v => translator.parse(v))
      case _ =>
        try {
          translator.parse(value)
        } catch {
          case e: Exception =>
            // logger.debug(s"  format:$format cannot parser data[$key,$value] see:$e")
            value
        }
    }


  }

}
