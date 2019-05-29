package com.haima.sage.bigdata.analyzer.aggregation.model

import java.util

import com.haima.sage.bigdata.analyzer.aggregation.math.MinHash

import scala.annotation.tailrec
import scala.collection.JavaConversions._

/**
  * 文本分词后的单元类
  */
case class Terms(line: String) {
  lazy val terms: List[String] = segmentText(line)
  lazy val minHash: Array[Long] = MinHash.minhash(terms)

  /**
    * 中国china,com.safge (1926)
    * [中国,china,',',com,.,raysdata,' ',(,1926,)]
    * Naive text tokenizer, split text by any non-alpha-non-number char. So a
    * Chinese string shall be split as a list of single-words, and in most
    * cases it is not much worse than intelligent Chinese segmentizers, which
    * seldom recognize longer Chinese terms.
    *
    * Note that whitespaces are discarded.
    * 文本分词
    *
    *
    * @param str
    * @return
    */
  final def segmentText(str: String): List[String] = {
    terms(str.toCharArray.toList)
    //    // 声明分词结果集合
    //    var list: List[String] = List[String]()
    //
    //    // 开始索引,默认值为0
    //    var beginIndex: Int = 0
    //
    //    // 循环文本中每个字符，根据规则拆分
    //    while (beginIndex < chars.length) {
    //      val c: Char = chars(beginIndex)
    //      val endIndex: Int = {
    //        val end = if (isAlphaNum(c)) {
    //          /*连续的(数字或英文字母),if it's a alphanum, find next alphanum*/
    //          chars.indexWhere(t => !isAlphaNum(t), from = beginIndex + 1)
    //        } else if (c > 127) {
    //          /*如果是中文等字符,合并成一个字段,it it's greater than 127, assuming it's chinese, find next*/
    //          //        // Assuming it's Chinese, maybe mixed with ASCII symbols
    //          //        MyStaticValue.isRealName = true
    //          //        val termList: java.util.List[Term] = ToAnalysis.parse(segment)
    //          //        termList.foreach(term => {
    //          //          list = list.:+(term.getRealName)
    //          //        })
    //
    //
    //          chars.indexWhere(_ <= 127, from = beginIndex + 1)
    //        } else {
    //          /* 默认是ASCII字符-自动字段,if first char is a symbol (<=127), add one character*/
    //          beginIndex + 1
    //        }
    //        if (end == -1) {
    //          chars.size
    //        } else {
    //          end
    //        }
    //      }
    //      val segment: String = str.substring(beginIndex, endIndex)
    //      beginIndex = endIndex
    //      list = list.:+(segment)
    //    }
    //    list
  }

  @inline
  @tailrec
  private final def terms(chars: List[Char], list: List[String] = List[String]()): List[String] = {
    chars match {
      case Nil =>
        list
      case head :: Nil =>
        list :+ head.toString
      case tails =>
        /*当前是字母数字时,找到不是字母数字的位置
        * 当前是字母是中文是,找到不是中文的位置
        * * 当前是字母是其他是,找到不是其他的位置
        * */
        val at = tails.indexWhere(one(tails.head), from = 1)
        val (data, tail) = tails.splitAt(if (at == -1) tails.size else at)
        terms(tail, list :+ new String(data.toArray))
    }
  }

  /**
    * 当that是A时,判断c一定不是A*/
  @inline private final def one(that: Char)(c: Char): Boolean = {
    if (isAlphaNum(that)) {
      !isAlphaNum(c)
    } else if (that > 127) {
      c <= 127
    } else {
      true
    }
  }

  // 是否为数字或英文字母
  @inline private final def isAlphaNum(c: Char): Boolean = ('0' <= c && c <= '9') || ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z')


  def distance(that: Terms): Int = {
    MinHash.distance(minHash, that.minHash)
  }

}
