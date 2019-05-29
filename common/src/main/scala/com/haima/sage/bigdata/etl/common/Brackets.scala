package com.haima.sage.bigdata.etl.common

/**
  * Created by zhhuiyan on 15/3/17.
  */
object Brackets {
  def unapply(str: String): Option[(String, String, String)] = {
    val from = str.indexOf("%{")
    val last = str.indexOf("}")
    if (from >= 0 && last - from > 1) {
      Some(str.substring(0, from), str.substring(from + 2, last), str.substring(last + 1))
    } else {
      None
    }

    /*reg.unapplySeq(str) match {
      case Some(start :: brackets :: end :: Nil) => Some(start, brackets, end)
      case _ => None
    }*/

  }


}

object QuoteString {
  private final val reg = "([\'\"]?)([\\s\\S]*?)\\1".r


  def unapply(str: String): Option[(String, String, String)] =
    reg.unapplySeq(str) match {
      case Some(quote :: brackets :: Nil) => Some(quote, brackets, quote)
      case _ => None
    }
}