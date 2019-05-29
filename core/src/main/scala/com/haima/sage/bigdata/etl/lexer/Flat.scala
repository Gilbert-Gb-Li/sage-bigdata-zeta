package com.haima.sage.bigdata.etl.lexer

import java.util
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.ParserProperties
import com.haima.sage.bigdata.etl.utils.Mapper

import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 16/4/29.
  */
object Flat extends Mapper {

  import scala.collection.JavaConversions._

  /* 转换成扁平化的数据 */
  @tailrec
  private final def flatMap(data: List[(String, Any)])(implicit result: java.util.Map[String, Any]): Unit = {

    data match {
      case Nil =>
      case head :: tails =>
        val next: List[(String, Any)] = head match {
          case (key, value: Map[String@unchecked, Any@unchecked]) =>
            result.put(key, mapper.writeValueAsString(value))
            tails ++ value.toList.map(tuple => (key + "." + tuple._1, tuple._2))
          case (key, value: java.util.Map[String@unchecked, Any@unchecked]) =>
            result.put(key, mapper.writeValueAsString(value))
            tails ++ value.toList.map(tuple => (key + "." + tuple._1, tuple._2))
          case (key, value: List[Any@unchecked]) =>
            value match {
              case Nil =>
                result.put(key, value)
                tails
              case itr =>
                /*合并数据为Map 或者string value*/
                val rt = itr.reduce[Any] {
                  case (first: Map[String@unchecked, Any@unchecked], second: Map[String@unchecked, Any@unchecked]) =>
                    val rt = new util.HashMap[String@unchecked, Any@unchecked]()
                    rt.putAll(first)
                    merge(List(Merger(rt, second.toList)))
                    rt
                  case (first: java.util.Map[String@unchecked, Any@unchecked], second: java.util.Map[String@unchecked, Any@unchecked]) =>
                    merge(List(Merger(first, second.toList)))
                    first
                  case (v1, v2) =>
                    v1 + "," + v2
                }
                result.put(key, mapper.writeValueAsString(rt))
                rt match {
                  case data: java.util.Map[String@unchecked, Any@unchecked] =>
                    tails ++ data.toList.map(tuple => (key + "." + tuple._1, tuple._2))
                  case _ =>
                    tails
                }
            }
          case (key, value: java.util.List[Any@unchecked]) =>
            value.toList match {
              case Nil =>
                result.put(key, value)
                tails
              case itr =>
                /*合并数据为Map 或者string value*/
                val rt = itr.reduce[Any] {
                  case (first: java.util.Map[String@unchecked, Any@unchecked], second: java.util.Map[String@unchecked, Any@unchecked]) =>
                    merge(List(Merger(first, second.toList)))
                    first
                  case (v1, v2) =>
                    v1 + "," + v2
                }
                result.put(key, mapper.writeValueAsString(rt))
                rt match {
                  case data: java.util.Map[String@unchecked, Any@unchecked] =>
                    tails ++ data.toList.map(tuple => (key + "." + tuple._1, tuple._2))
                  case _ =>
                    tails
                }
            }


          case (key, value) =>
            result.put(key, value)
            tails
        }
        flatMap(next)

    }

  }

  def flatMap(map: Map[String, Any]): java.util.Map[String, Any] = {
    val result = new util.HashMap[String, Any]()
    flatMap(map.toList)(result)
    result
  }


  case class Merger(first: java.util.Map[String, Any], second: List[(String, Any)])

  /*
  * 合并两个Map
  * */
  @tailrec
  private final def merge(data: List[Merger]): Unit = {
    data match {
      case Nil =>
      case one :: tails =>
        val rt: List[Merger] = one.second match {
          case Nil =>
            tails
          case head :: tail =>
            head match {
              case (k, v: java.util.List[Any@unchecked]) =>
                one.first.put(k, one.first.get(k) match {
                  case null =>
                    v
                  case d: java.util.List[Any@unchecked] =>
                    d.add(v)
                    d
                  case d: String =>
                    d + "," + v
                  case d =>
                    v.add(d)
                })
                tails.+:(Merger(one.first, tail))
              case (k, v: java.util.Map[String@unchecked, Any@unchecked]) =>
                one.first.get(k) match {
                  case null =>
                    one.first.put(k, v)
                    tails.+:(Merger(one.first, tail))
                  case d: String =>
                    one.first.put(k, d + "," + v)
                    tails.+:(Merger(one.first, tail))
                  case d: java.util.Map[String@unchecked, Any@unchecked] =>
                    one.first.put(k, v)
                    /*广度优先遍历*/
                    tails.+:(Merger(one.first, tail)).+:(Merger(v, d.toList))
                  case d=>
                    one.first.put(k, d + "," + v)
                    tails.+:(Merger(one.first, tail))
                }
              case (k, v) =>
                one.first.put(k, one.first.get(k) match {
                  case null =>
                    v
                  case d: String =>
                    d + "," + v
                  case d: java.util.List[Any@unchecked] =>
                    d.add(v)
                    d
                  case d =>
                    d + "," + v

                })
                tails.+:(Merger(one.first, tail))


            }

        }
        merge(rt)
    }


  }


  /*将map数据转换成属性*/
  @tailrec
  private final def add2Properties(data: List[(String, Any)])(implicit result: java.util.List[ParserProperties]): Unit = {

    data match {
      case Nil =>
      case head :: tails =>
        val next: List[(String, Any)] = head match {
          case (key, value: Map[String@unchecked, Any@unchecked]) =>
            result.add(ParserProperties(key = key, `type` = "object", sample = None))
           // result.add(ParserProperties(key = key, `type` = "object", sample = Some(mapper.writeValueAsString(value))))
            tails ++ value.toList.map(tuple => (key + "." + tuple._1, tuple._2))
          case (key, value: java.util.Map[String@unchecked, Any@unchecked]) =>
            result.add(ParserProperties(key = key, `type` = "object", sample = None))
            //result.add(ParserProperties(key = key, `type` = "object", sample = Some(mapper.writeValueAsString(value))))
            tails ++ value.toList.map(tuple => (key + "." + tuple._1, tuple._2))
          case (key, value: List[Any@unchecked]) =>
            value match {
              case Nil =>
                result.add(ParserProperties(key = key, `type` = "list[object]"))
                tails
              case itr =>
                /*合并数据为Map 或者string value*/
                val rt = itr.reduce[Any] {
                  case (first: Map[String@unchecked, Any@unchecked], second: Map[String@unchecked, Any@unchecked]) =>

                    val rt = new util.HashMap[String@unchecked, Any@unchecked]()
                    rt.putAll(first)
                    merge(List(Merger(rt, second.toList)))
                    rt
                  case (first: java.util.Map[String@unchecked, Any@unchecked], second: java.util.Map[String@unchecked, Any@unchecked]) =>
                    merge(List(Merger(first, second.toList)))
                    first
                  case (v1, v2) =>
                    v1 + "," + v2
                }
                result.add(ParserProperties(key = key, `type` = "list[object]", sample =None))
                // result.add(ParserProperties(key = key, `type` = "list[object]", sample = Some(mapper.writeValueAsString(rt))))
                rt match {
                  case data: java.util.Map[String@unchecked, Any@unchecked] =>
                    tails ++ data.toList.map(tuple => (key + "." + tuple._1, tuple._2))
                  case _ =>
                    tails
                }
            }
          case (key, value: java.util.List[Any@unchecked]) =>
            value.toList match {
              case Nil =>
                result.add(ParserProperties(key = key, `type` = "list[object]"))
                tails
              case itr =>
                /*合并数据为Map 或者string value*/
                val rt = itr.reduce[Any] {
                  case (first: java.util.Map[String@unchecked, Any@unchecked], second: java.util.Map[String@unchecked, Any@unchecked]) =>
                    merge(List(Merger(first, second.toList)))
                    first
                  case (v1, v2) =>
                    v1 + "," + v2
                }
                result.add(ParserProperties(key = key, `type` = "list[object]", sample = None))
              //  result.add(ParserProperties(key = key, `type` = "list[object]", sample = Some(mapper.writeValueAsString(rt))))
                rt match {
                  case data: java.util.Map[String@unchecked, Any@unchecked] =>
                    tails ++ data.toList.map(tuple => (key + "." + tuple._1, tuple._2))
                  case _ =>
                    tails
                }
            }
          case (key, value: Int) =>
            result.add(ParserProperties(key = key, `type` = "integer", sample = Option(value.toString)))
            tails
          case (key, value: Long) =>
            result.add(ParserProperties(key = key, `type` = "long", sample = Option(value.toString)))
            tails
          case (key, value: Double) =>
            result.add(ParserProperties(key = key, `type` = "double", sample = Option(value.toString)))
            tails

          case (key, value: Float) =>
            result.add(ParserProperties(key = key, `type` = "float", sample = Option(value.toString)))
            tails
          case (key, value: Date) =>
            result.add(ParserProperties(key = key, `type` = "datetime", sample = Option(value.toString)))
            tails
          case (key, value) =>
            result.add(ParserProperties(key = key, `type` = "string", sample = Option(if (value == null) null else value.toString)))
            tails
        }
        add2Properties(next)

    }
  }

  /*将map数据转换成属性*/
  def toProperties(map: Map[String, Any]): List[ParserProperties] = {
    val result = new java.util.ArrayList[ParserProperties]()
    add2Properties(map.toList)(result)
    result.toList
  }
}
