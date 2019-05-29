package com.haima.sage.bigdata.etl.normalization.format

import com.haima.sage.bigdata.etl.utils.{DictionaryTable, Logger}

/**
  * Created by zhhuiyan on 16/8/22.
  */
case class IPTranslator() extends Translator[java.util.Map[String, Any]] with Serializable with Logger {

  override def parse(value: Any): java.util.Map[String, Any] = {
    import scala.collection.JavaConversions._
    value match {
      case i: java.util.Map[String @unchecked, Any @unchecked] =>
        i
      case i: String =>

        ( DictionaryTable.find(i) + ("ip" -> value))
      case _ =>
        logger.debug(s"unsupport ip[$value,${value.getClass}] for Translate ")
        Map("ip" -> value)
    }


  }

}
