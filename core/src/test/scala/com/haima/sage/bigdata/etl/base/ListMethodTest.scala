package com.haima.sage.bigdata.etl.base

import java.util.{Calendar, Date}

import org.junit.Test

/**
  * Created by zhhuiyan on 2016/9/28.
  */
class ListMethodTest {
  @Test
  def testRightOpt {
    var list = 1 :: 2 :: 3 :: Nil
    println(0 :: list)
    println(list.slice(0, list.size - 1))
  }

  @Test
  def testPlus(): Unit = {
    println("erwer")
    println(List(1, 2, 3) ++ List(4, 5, 6))

    //var map = Map[String, String]()
    var map = Map[String, String]("name" -> "jason", "age" -> "500", "test_100" -> "test_100", "test_101" -> "test_101")
    map += ("city" -> "北京")
    map += ("test" -> "能添加吗")

    map += ("success" -> "添加成功了吗", "anthor" -> "另外一个")
    val aa = List("city", "name")
    //map -= ("city","name")
    //map -=(List("city","name").toSet)
    val map3 = map.filter { case (key, value) =>
      (value != null) && (!aa.contains(key.trim.toLowerCase)) //todo fixed 确定大小写敏感问题的处理方案
    }

    val keySet3 = map3.keys
    val key_iter3 = keySet3.iterator //遍历,迭代map;
    while (key_iter3.hasNext) {
      val key = key_iter3.next
      println(key + ":" + map3.get(key).get)
    }
    println("====================")
    val keySet = map.keys
    val key_iter = keySet.iterator //遍历,迭代map;
    while (key_iter.hasNext) {
      val key = key_iter.next
      println(key + ":" + map.get(key).get)
    }

    println("====================")
    println(map.values++map3.values)
  }
}
