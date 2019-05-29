package com.haima.sage.bigdata.analyzer.aggregation.model

import java.io.IOException

import com.haima.sage.bigdata.etl.utils.Logger

import scala.collection.mutable
//import org.ansj.domain.Term
//import org.ansj.splitWord.analysis.ToAnalysis
//import org.ansj.util.MyStaticValue


/**
  * Created by wxn on 17/10/26.
  */
case class LogReducer(minMatched: Double = 0.7 /*设置最小匹配度*/) extends Logger {

  /**
    * 比较两个String集合是否相等
    *
    * @param a
    * @param b
    * @return
    */
  def equals(a: List[String], b: List[String]): Boolean = {
    if (a.lengthCompare(b.size) != 0) {
      return false
    }

    if (!a.equals(b)) {
      return false
    }

    true
  }

  /*合并两个聚类,如果边际密度合适*/
  def reduce: (Set[Cluster], Set[Cluster]) => Set[Cluster] = {
    (clusters, c) =>
      val data1 = c.map(cluster => {
        clusters.find(cluster.similar) match {
          case Some(temp) =>
            cluster.merge(temp)
          case _ =>
            cluster

        }
      })

      val data = data1 ++ clusters
      data


  }

  def cluster(termsList: List[(Int, Terms)]): List[Cluster] = {
    val clusters: mutable.MutableList[Cluster] = mutable.MutableList[Cluster]()
    var tail: List[(Int, Terms)] = termsList


    val head = tail.head

    val c = Cluster(0, dist = minMatched, elements = List(head))
    val c2 = tail.tail.foldLeft(c)((c, second) => c.add(second))
    clusters += c2
    //    while (tail.nonEmpty) {
    //      tail match {
    //        case head :: tails =>
    //          val id = clusters.size + 1
    //          val cluster: Cluster = Cluster(id, dist = minMatched, elements = List(head))
    //
    //          val (merge, tail1) = tails.span(second => cluster.within(second._2))
    //
    //
    //          tail = tail1
    //          clusters += merge.foldLeft(cluster)((c, second) => c.add(second))
    //        case Nil =>
    //      }
    //    }

    clusters.toList
  }


  @throws[IOException]
  def runLogReducer(rows: List[String]): Map[List[Pattern], String] = {
    // Split each line into terms, calculate their min-hash
    val termsList = rows.zipWithIndex.map(tuple => (tuple._2 + 1, Terms(tuple._1))).toList
    val clusters: List[Cluster] = cluster(termsList)

    // Matching and clustering within eaclusterch
    clusters.map(cluster => {
      cluster.getPattern(minMatched)
    }).toMap
  }
}
