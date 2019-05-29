package com.haima.sage.bigdata.etl.utils

import org.junit.Test

/**
  * Created by zhhuiyan on 2017/2/8.
  */
class MapTranstor {
  @Test
  def transtor(): Unit = {

    /*   val org = Map("0" -> Map("A" -> 1, "B" -> 2, "C" -> 3), "1" -> Map("B" -> 2, "C" -> 1), "2" -> Map("C" -> 3))*/


    /*val data = org.map {
      case (key, value) =>
        value.map {
          case (_key, _value) =>
            ((_key, key), _value)
        }.toList
    }.reduce[List[((String, String), Int)]] {
      case (first, second) =>
        first ++ second
    }.toMap*/
    val org = (0 until 1000).map {
      i =>
        (i.toString, Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J").map(j =>
          (j.toString, 1)).toMap)
    }.toMap


    println(System.currentTimeMillis())
    val data = org.map {
      case (key, value) =>
        value.map {
          case (_key, _value) =>
            ((_key, key), _value)
        }.toList
    }.reduce[List[((String, String), Int)]] {
      case (first, second) =>
        first ++ second
    }.toMap


    val datas = (0 until 1000).map {
      i =>
        Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J").map(j =>
          ((j.toString, i.toString), 0)).toList
    } reduce[List[((String, String), Int)]] {
      case (first, second) =>
        first ++ second
    }

    val rt = datas.map {
      case (key, value) =>
        (key, data.getOrElse(key, 0))
    }.groupBy(_._1._1).map {
      case (key, value) =>
        (key, value.map(_._2))
    }
    println(System.currentTimeMillis())
    println(rt)
    //println(data.groupBy(_._1._1))
  }

}
