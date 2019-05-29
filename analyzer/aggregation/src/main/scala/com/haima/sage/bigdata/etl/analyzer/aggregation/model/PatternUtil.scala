package com.haima.sage.bigdata.analyzer.aggregation.model

import org.apache.commons.collections4.ListUtils

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Created by lenovo on 2018/2/27.
  */
object PatternUtil {

  private final val LINE_FEEDER: String = "\n"
  /**
    * Convert the LCS result into a more human readable pattern/params format.
    *
    * Assuming there are 2 logs:
    *             10.1.1.1 connecting to 192.168.1.1
    *             10.1.1.2 connecting to 192.168.1.2
    *
    * Then after first round of LCS, the input of pattern shall be
    * 'connecting to'
    *
    * Now, the pattern shall be merged as
    * <PARAM>connecting to<PARAM> And the
    * parameters of the two logs shall be printed as
    *             10.1.1.1<PATTERN>192.168.1.1
    *             10.1.1.2<PATTERN>192.168.1.2
    *
    * @return
    *
    **/
  @inline private def mergePattern(pattern: List[String], matchedLogs: List[List[String]]): Pattern = {
    if (matchedLogs.isEmpty) {
      // No matches found, the pattern shall be the segmented terms of the
      // only log
      Pattern(List(PatternText(pattern.mkString)))
    } else {
      val paramPlaceholder: String = "\u0000"


      //  val patternPlaceholder: String = "\u0001"

      // Merge and extract parameters of each matched log

      /*
      * sample data List[10,.,1.,1.,1_aa,:,:,bb]
      * * pattern List[_,:,:]
      *
      *  merge as  List[10.1.1.1,_,aa,::,bb]
      *
      *  pattern  List[_,::]
      *
      * */
      val (mergedPattern, _matchedLogs) = pattern.foldLeft((List[String](), matchedLogs)) {
        case ((list, logs), seg) =>
          var placeholderNeeded: Boolean = false
          val _logs = logs.map(log => {
            val pos: Int = log.indexOf(seg)
            placeholderNeeded = placeholderNeeded | (pos != 0)
            log.slice(pos + 1, log.size)
          }

          )
          if (placeholderNeeded) {
            (list :+ paramPlaceholder :+ seg, _logs)
          } else {
            (list.:+(seg), _logs)
          }

      }
      val withLastPattern = if (_matchedLogs.exists(_.nonEmpty)) {
        mergedPattern.:+(paramPlaceholder)
      } else {
        mergedPattern
      }


      val pts: (List[PatternElement], Int) = withLastPattern.foldLeft[(List[PatternElement], Int)]((List(), 0)) {
        case ((list, index), `paramPlaceholder`) =>
          (list :+ PatternParameter(String.valueOf(index)), index + 1)
        case ((Nil, index), text) =>
          (Nil :+ PatternText(text), index)
        case ((list, index), text) if list.last.isInstanceOf[PatternText] =>
          val _text: PatternText = list.last.asInstanceOf[PatternText]
          (list.slice(0, list.size - 1) :+ PatternText(_text.text.concat(text)), index)
        case ((list, index), text) =>
          (list :+ PatternText(text), index)

      }
      val lrp: Pattern = Pattern(pts._1)
      lrp
    }
  }

  def getPattern(implicit minMatched: Double = 0.7,first:List[Terms]): (List[Pattern], String) = {
    val buffer: StringBuffer = new StringBuffer()
    val tmpPatternList: mutable.MutableList[Pattern] = mutable.MutableList[Pattern]()
    // discard single instance
    if (first.size < 0) {
      return (Nil, "")
    } else if (first.size == 1) {
      return (List(mergePattern(first.head.terms, Nil)), first.head.line)
    } else {
      var loop1: List[Terms] = first
      while (loop1.nonEmpty) {
        loop1 match {
          case head :: tails =>
            var terms: List[String] = head.terms
            loop1 = tails
            var loop2 = loop1
            val minLCSLen: Int = (minMatched * terms.size).toInt
            val copyOfFirst: List[String] = List[String]() ::: terms
            var lcsList: List[List[String]] = List[List[String]]()
            var matchedLogs: List[List[String]] = List[List[String]]()

            while (loop2.nonEmpty) {

              var maxLCS: List[String] = Nil
              // Match all
              lcsList = loop2.map(second => {
                var lcs: List[String] = Nil
                if (second.terms.size >= minLCSLen) {
                  lcs = ListUtils.longestCommonSubsequence(terms, second.terms).toList

                  if (lcs.size < minLCSLen || lcs.size < (minMatched * second.terms.size)) {
                    lcs = Nil
                  }
                  if (maxLCS.size < lcs.size) {
                    maxLCS = lcs
                  }
                }
                lcs
              })

              if (maxLCS.equals(Nil)) {
                loop2 = Nil
              } else {

                // Remove strings with maximal match
                var tmpTailsList: List[Terms] = loop2
                var tmpTermsList: List[Terms] = List[Terms]()


                lcsList.foreach(lcs => {
                  if (lcs.equals(maxLCS)) {
                    matchedLogs = matchedLogs.:+(tmpTailsList.head.terms)
                  } else {
                    tmpTermsList = tmpTermsList.:+(tmpTailsList.head)
                  }
                  tmpTailsList = tmpTailsList.tail
                })

                loop2 = tmpTermsList
                loop1 = loop2
                terms = maxLCS
              }
            }

            buffer.append("Total " + (matchedLogs.size + 1) + " matched")
            buffer.append(LINE_FEEDER)

            for (str <- copyOfFirst) {
              buffer.append(str)
            }
            buffer.append(LINE_FEEDER)

            for (log <- matchedLogs) {
              for (token <- log) {
                buffer.append(token)
              }
              buffer.append(LINE_FEEDER)
            }
            val pattern: Pattern = mergePattern(terms, matchedLogs)

            if (pattern != null) {

              tmpPatternList += pattern.copy(total = matchedLogs.size + 1)
              buffer.append("Pattern: " + pattern)
            }
            buffer.append(LINE_FEEDER)
            buffer.append(LINE_FEEDER)
          case Nil =>
        }
      }
    }


    //    tmpPatternList.size - sizeBefore
    (tmpPatternList.toList, buffer.toString)
  }
}
