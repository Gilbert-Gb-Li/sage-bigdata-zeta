package com.haima.sage.bigdata.analyzer.sql.streaming

import com.haima.sage.bigdata.analyzer.sql.utils.Lang
import org.junit.Test

/**
  * Created by chengji on 17-8-8.
  */
class SqlTest {
  final val pattern = "[\u4e00-\u9fa5]".r
  final val replace = "(\\w)\\s+(like|LIKE)\\s+'(%?(\\w|[\\u4e00-\\u9fa5])*%?)'".r

  implicit def containChinese(implicit str: String): Boolean = {
    pattern.findFirstIn(str).isDefined
  }

  @Test
  def test(): Unit = {
    var s = "i m 中国chinese"
    println(isContainChinese(s))
    s = cnToUnicode(s)
    println(s)
    s = unicodeToCn(s)
    println(s)
  }

  import java.util.regex.Pattern

  def isContainChinese(str: String): Boolean = {
    val p = Pattern.compile("[\u4e00-\u9fa5]")
    val m = p.matcher(str)
    if (m.find)
      true
    else
      false
  }

  private def unicodeToCn(unicode: String): String = {
    /** 以 \ u 分割，因为java注释也能识别unicode，因此中间加了一个空格 */
    val strs = unicode.split("\\\\")
    var returnStr = ""
    // 由于unicode字符串以 \ u 开头，因此分割出的第一个字符是""。
    var i = 1
    while ( {
      i < strs.length
    }) {
      returnStr += Integer.valueOf(strs(i), 16).intValue.toChar

      {
        i += 1;
        i - 1
      }
    }
    returnStr
  }

  private def cnToUnicode(cn: String): String = {
    val chars = cn.toCharArray
    var returnStr = ""
    var i = 0
    while ( {
      i < chars.length
    }) {
      returnStr += "\\" + Integer.toString(chars(i), 16)

      {
        i += 1;
        i - 1
      }
    }
    returnStr
  }

  @Test
  def testSubstring(): Unit = {
    println("%你好%".substring(1, "%你好%".length - 1))
    println("%你好".substring(1))
    println("你好%".substring(0, "你好%".length - 1))
  }

  @Test
  def reg(): Unit = {
    val sql = "select a,b,c from test where b like '你好%' and c like '%人民%'"

    if (sql.toLowerCase.contains(" like ")) {
      //如果包含like语句
      val p = Pattern.compile("(\\w)\\s+(like|LIKE)\\s+'(%?(\\w|[\\u4e00-\\u9fa5])*%?)'")
      val m = p.matcher(sql)
      var newSql = sql
      var i = 0
      while (m.find()) {
        val funName = s"zlike$i" //获取udf名称
        println(m.group(3))
        val subSql = s"$funName(${m.group(1)})" //拼接子串
        newSql = newSql.replace(m.group(0), subSql) //替换子串
        i += 1
      }
      println(newSql)
    } else {
      println(sql)
    }
  }
}
