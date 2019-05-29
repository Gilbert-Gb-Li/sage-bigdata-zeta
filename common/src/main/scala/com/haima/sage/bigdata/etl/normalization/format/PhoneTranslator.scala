package com.haima.sage.bigdata.etl.normalization.format

import com.haima.sage.bigdata.etl.utils.{Logger, PhoneExt}

/**
  * Created by zhhuiyan on 16/8/22.
  */
case class PhoneTranslator() extends Translator[java.util.Map[String, Any]] with Serializable with Logger {

  override def parse(value: Any): java.util.Map[String, Any] = {
    import scala.collection.JavaConversions._
    value match {
      case i: java.util.Map[String @unchecked, Any @unchecked] =>
        i
      case i: String =>
        (PhoneExt.find(i) + ("phone" -> value))
      case _ =>
        logger.debug(s"unsupport phone[$value] for Translate ")
        Map("phone" -> value)
    }
  }
}
