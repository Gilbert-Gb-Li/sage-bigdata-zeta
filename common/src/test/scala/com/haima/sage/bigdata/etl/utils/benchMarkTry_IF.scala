package com.haima.sage.bigdata.etl.utils

import java.util.regex.{Matcher, Pattern}

import org.junit.Test

class benchMarkTry_IF {

  val p: Pattern = Pattern.compile("-?\\d+")

  @Test
  def test_ifM() {


    val start = System.currentTimeMillis()
    (0 to 10000000).foreach(i => {
      val str_v = i.toString
      if (str_v.matches("-?\\d+"))
        str_v.toInt
      else{
        0
      }
    })
    println(System.currentTimeMillis() - start)
    (0 to 10000000).foreach(i => {
      val str_v = "a" + i.toString
      if (str_v.matches("-?\\d+"))
        str_v.toInt
      else {
        0
      }
    })

    println(System.currentTimeMillis() - start)
  }
  @Test
  def test_if() {


    val start = System.currentTimeMillis()
    (0 to 10000000).foreach(i => {
      val str_v = i.toString
      val m: Matcher = p.matcher(str_v)
      if (m.matches)
        str_v.toInt
      else{
        0
      }
    })
    println(System.currentTimeMillis() - start)
    (0 to 10000000).foreach(i => {
      val str_v = "a" + i.toString
      val m: Matcher = p.matcher(str_v)
      if (m.matches)
        str_v.toInt
      else {
        0
      }
    })

    println(System.currentTimeMillis() - start)
  }

  @Test
  def test_try() {

    val start = System.currentTimeMillis()
    (0 to 10000000).foreach(i => {
      val str_v = i.toString
      try {
        str_v.toInt
      }catch {
        case e=>
          0
      }
    }

    )
    println(System.currentTimeMillis() - start)
    (0 to 10000000).foreach(i => {
      val str_v = "a"+i.toString
      try {
        str_v.toInt
      }catch {
        case e=>
          0
      }
    }

    )
    println(System.currentTimeMillis() - start)

  }

}
