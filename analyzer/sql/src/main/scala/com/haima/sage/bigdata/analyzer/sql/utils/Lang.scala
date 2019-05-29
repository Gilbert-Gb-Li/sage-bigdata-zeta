package com.haima.sage.bigdata.analyzer.sql.utils

object Lang {
  private final val pattern = "[\u4e00-\u9fa5]".r
  //final val replace = "(\\w)\\s+(like|LIKE)\\s+'(%?(\\w|[\\u4e00-\\u9fa5])*%?)'".r

  implicit def containChinese(implicit str: String): Boolean = {
    pattern.findFirstIn(str).isDefined
  }


  /**
    * 将遇到的中文字符转换成unicode
    */

  implicit def unicode(implicit cn: String): String = {
   val rt= pattern.replaceAllIn(cn, m =>
      m.group(0).toCharArray.map(c => "\\\\u" + Integer.toString(c, 16)).reduce(_ + _))
  println(rt)
    cn
  }
}
