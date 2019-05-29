package com.haima.sage.bigdata.analyzer.aggregation.model

import org.apache.commons.collections4.ListUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable


case class Cluster(id: Int, name: String = "", dist: Double = 0.7, elements: List[(Int, Terms)]) extends Serializable {

  lazy val logger = LoggerFactory.getLogger(classOf[Cluster])


  // 换行符
  private final val LINE_FEEDER: String = "\n"

  /*合并两个类,并返回新的*/
  def merge(that: Cluster): Cluster = {
    that.elements.foldLeft(this)((c, terms) => c.add(terms))
  }

  /*两个类相似*/
  def similar(that: Cluster): Boolean = {
    that.elements.exists(element => within(element._2))
  }

  /*数据距离合适*/
  def within(second: Terms): Boolean = {
    // << 6 = * sizeof(long)
    val minDist: Int = ((1 - dist + 0.05) * (second.minHash.length << 6)).toInt
    elements.forall(first => {
      val hammingDist: Int = first._2.distance(second)
      hammingDist <= minDist
    })
  }

  def add(terms: (Int, Terms)): Cluster = {
    this.copy(elements = elements :+ terms)
  }

  def add(c: Cluster): Cluster = {
    c.elements.foldLeft(this)((c, terms) => c.add(terms))
  }

  def size: Int = elements.size

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
      logger.debug(lrp.toString)
      lrp
    }
  }

  def getPattern(implicit minMatched: Double = 0.7): (List[Pattern], String) = {
    val buffer: StringBuffer = new StringBuffer()
    val tmpPatternList: mutable.MutableList[Pattern] = mutable.MutableList[Pattern]()
    // discard single instance
    if (this.size < 0) {
      return (Nil, "")
    } else if (this.size == 1) {
      return (List(mergePattern(this.elements.head._2.terms, Nil)), this.elements.head._2.line)
    } else {
      var loop1: List[Terms] = this.elements.map(_._2)
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

            logger.debug("")
            logger.debug("Total " + (matchedLogs.size + 1) + " matched")

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

  override def hashCode(): Int = this.id

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case cluster: Cluster if obj != null =>
        this.id.equals(cluster.id)
      case _ =>
        false
    }

  }
}
