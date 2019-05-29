package com.haima.sage.bigdata.etl.normalization.format

import com.haima.sage.bigdata.etl.utils.{IDCardExt, Logger}

/**
  * Created by zhhuiyan on 16/8/22.
  */
case class IDCardTranslator() extends Translator[java.util.Map[String, Any]] with Serializable with Logger {

  override def parse(value: Any): java.util.Map[String, Any] = {
    import scala.collection.JavaConversions._
    value match {
      case i: java.util.Map[String @unchecked, Any @unchecked] =>
        i
      case i: String =>
        (IDCardExt.find(i) + ("idcard" -> value))
      case _ =>
        logger.debug(s"unsupport idcard[$value] for Translate ")
        Map("idcard" -> value)
    }


  }

}
