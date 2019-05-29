package com.haima.sage.bigdata.etl.utils

import org.reflections.Reflections

import scala.util.Try
import scala.util.matching.Regex

object ClassUtils {

  class ClassUtils



  final def subClass[T](underlying: Class[T], params: Int = 1,name:String="com.haima"): Map[Class[_], Class[_ <: T]] = {
    import scala.collection.JavaConverters._

    val reflects = new Reflections(name)
    reflects.getSubTypesOf(underlying).asScala.toSeq.filter(t => {
      !t.isInterface && t.getConstructors.length > 0 &&
        t.getConstructors()(0).getParameterTypes.length >= params &&
        !t.getConstructors()(0).getParameterTypes()(0).isInterface
    }).map(t => {
      (t.getConstructors()(0).getParameterTypes()(0), t)
    }).toMap
  }


  object ParamType {

    final val regex: Regex =""".*\(\w+: ([^,\)]*).*""".r

    def unapply(str: String): Option[Class[_]] = {
      regex.unapplySeq(str) match {
        case Some(name :: Nil) =>
          Try(Class.forName(name)).toOption
        case _ =>
          None
      }
    }

  }

}
