package com.haima.sage.bigdata.etl.common.model

import java.util

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.collection.{GenMap, GenTraversableOnce}

object MapUtils {
  def apply(data: Map[String, Any] = Map()): RichMap =  RichMap(data)

  import scala.collection.JavaConversions._

  def complexOnce: Any => Any = {
    case sv: Map[String@unchecked, Any@unchecked] =>
      apply(sv).complex().toMap
    case sv: java.util.Map[String@unchecked, Any@unchecked] =>
      apply(sv.toMap).complex().toMap
    case d =>
      d
  }


  /**
    * 对于map数据获取key对应的数据,然后执行func(key对应的数据,使用真实的fk)
    *
    * @param key   map的key
    * @param isAdd 是否是添加
    * @param func  调用的函数
    */
  def recursionJava(d: util.Map[String, Any], key: String, isAdd: Boolean = false)(func: (java.util.Map[String, Any], String) => Unit): util.Map[String, Any] = {
    operation(List((d, key.split("\\."))), isAdd = isAdd)(func)
    d
  }

  private def toJava(data: scala.collection.convert.Wrappers.MapWrapper[String@unchecked, Any@unchecked]): java.util.Map[String, Any] = {
    val _data = new util.HashMap[String, Any]()
    _data.putAll(data)
    _data

  }

   def toJava(data: Map[String, Any]): java.util.Map[String, Any] = {
    val _data = new util.HashMap[String, Any]()
    data.foreach {
      case (key, v) =>
        _data.put(key, v)
    }
    _data
  }

  @tailrec
  private[model] final def operation(data: List[(java.util.Map[String, Any], Array[String])], isAdd: Boolean = false)
                             (func: (java.util.Map[String, Any], String) => Unit): Unit = {

    data match {
      case Nil =>
      case first :: tails =>
        val result: List[(java.util.Map[String, Any], Array[String])] = first._2.toList match {
          case head :: Nil =>
            func(first._1, head)

            tails
          case head :: tail =>
            first._1.get(head) match {
              case sub: scala.collection.convert.Wrappers.MapWrapper[String@unchecked, Any@unchecked] =>
                val rt = toJava(sub)
                first._1.put(head, rt)
                tails.+:(rt, tail.toArray)
              case sub: Map[String@unchecked, Any@unchecked] =>
                val rt = toJava(sub)
                first._1.put(head, rt)
                tails.+:(rt, tail.toArray)
              case sub: java.util.Map[String@unchecked, Any@unchecked] =>
                first._1.put(head, sub)
                tails.+:(sub, tail.toArray)
              case data: List[Any@unchecked] =>
                val rt = data.filter(d => {
                  d.isInstanceOf[java.util.Map[String@unchecked, Any@unchecked]] || d.isInstanceOf[Map[String@unchecked, Any@unchecked]]
                }).map {
                  case data: scala.collection.convert.Wrappers.MapWrapper[String@unchecked, Any@unchecked] =>
                    (toJava(data), tail.toArray)
                  case data: Map[String@unchecked, Any@unchecked] =>
                    (toJava(data), tail.toArray)
                  case data: java.util.Map[String@unchecked, Any@unchecked] =>
                    (data, tail.toArray)
                }
                first._1.put(head, rt.map(_._1).toList)
                tails.++(rt)
              case data: java.util.List[Any@unchecked] =>
                val rt = data.filter(d => {
                  d.isInstanceOf[java.util.Map[String@unchecked, Any@unchecked]] || d.isInstanceOf[Map[String@unchecked, Any@unchecked]]
                }).map {
                  case data: scala.collection.convert.Wrappers.MapWrapper[String@unchecked, Any@unchecked] =>
                    (toJava(data), tail.toArray)
                  case data: Map[String@unchecked, Any@unchecked] =>
                    (toJava(data), tail.toArray)
                  case data: java.util.Map[String@unchecked, Any@unchecked] =>
                    (data, tail.toArray)
                }

                first._1.put(head, rt.map(_._1).toList)
                tails.++(rt)
              case _ =>
                /*
                *
                * 强制更新值,或者补充值,如果是更新或者删除不需要
                * */
                if (isAdd) {
                  val m = new util.HashMap[String, Any]()
                  first._1.put(head, m)
                  tails.+:(m, tail.toArray)
                } else {
                  tails
                }

            }
          case _ =>
            tails

        }

        operation(result, isAdd)(func)
    }

  }
}

case class RichMap(data: Map[String, Any] = Map()) extends Map[String, Any] with GenMap[String, Any] with Serializable {

  import MapUtils._

  import scala.collection.JavaConversions._


  def toMap: Map[String, Any] = data


  override def ++[B1 >: Any](xs: GenTraversableOnce[(String, B1)]): RichMap = RichMap(super.++(xs))

  def complex(): RichMap = {
    RichMap(data.map {
      case (key, v: Map[String@unchecked, Any@unchecked]) =>
        (key, complexOnce(v))
      case (key, v: java.util.Map[String@unchecked, Any@unchecked]) =>
        (key, complexOnce(v))
      case (key, v: java.util.List[Any@unchecked]) =>
        (key, v.map(complexOnce))
      case (key, v: List[Any@unchecked]) =>
        (key, v.map(complexOnce))
      case (key, v: ArrayBuffer[Any@unchecked]) =>
        (key, v.map(complexOnce).toList)
      case (key, v) =>
        if (key.contains(".")) {
          val keys = key.split("\\.", 2)
          (keys(0), complexOnce(Map(keys(1) -> v)))
        } else {
          (key, v)
        }
    })

  }


  private def recursionGet(field: String): Option[Any] = {

    data.get(field) match {
      case Some(d) =>
        Option(d)
      case _ if field.contains(".") =>
        val rt = new util.ArrayList[Any]()
        recursion(field) {
          case (dd, k) =>
            dd.get(k) match {
              case null =>
              case v =>
                rt.add(v)
            }
        }
        if (rt.isEmpty) {
          None
        } else if (rt.size() == 1) {
          Some(rt.get(0))
        } else {
          Some(rt)
        }
      case _ =>
        None
    }


  }


  /**
    * 对于map数据获取key对应的数据,然后执行func(key对应的数据,使用真实的fk)
    *
    * @param key   map的key
    * @param isAdd 是否是添加
    * @param func  调用的函数
    */
  def recursion(key: String, isAdd: Boolean = false)(func: (java.util.Map[String, Any], String) => Unit): RichMap = {
    val d = toJava(data)
    operation(List((d, key.split("\\."))), isAdd = isAdd)(func)

    RichMap(d.toMap)
  }


  private def recursionPut(key: String, value: Any): RichMap = {
    if (!key.contains(".")) {
      RichMap(data + (key -> value))
    } else {
      recursion(key, isAdd = true) {
        case (dd, k) =>
          dd.get(k) match {
            case null =>
              dd.put(k, value)
            case d: java.util.List[Any@unchecked] =>
              d.add(value)
            case d: List[Any@unchecked] =>
              dd.put(k, d.+:(value))
            case _ =>
              dd.put(k, value)
          }

      }
    }
  }


  override def +[B1 >: Any](kv: (String, B1)): RichMap = {
    recursionPut(kv._1, kv._2)
  }

  override def get(key: String): Option[Any] = recursionGet(key)

  override def iterator: Iterator[(String, Any)] = {
    data.iterator
  }

  override def -(key: String): RichMap = {

    if (!key.contains(".")) {
      RichMap(data - key)

    } else {
      var isEmpty = false
      val keys = key.split("\\.")

      val d = toJava(data)
      operation(List((d, keys))) {
        case (dd, k) =>
          dd.remove(k)
          isEmpty = dd.isEmpty


      }
      /*去掉空值*/
      if (isEmpty) {
        (1 until keys.length).foreach {
          i =>
            operation(List((d, keys.slice(0, keys.length - i)))) {
              case (_dd, _k) =>
                _dd.get(_k) match {
                  case d: GenTraversableOnce[_] if d.isEmpty =>
                    _dd.remove(_k)
                  case d: java.util.Map[String@unchecked, Any@unchecked] if d.isEmpty =>
                    _dd.remove(_k)
                  case _ =>
                }
            }
        }
      }

      RichMap(d.toMap)
    }


  }
}
