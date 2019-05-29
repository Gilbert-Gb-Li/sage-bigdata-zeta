package com.haima.sage.bigdata.etl.utils

import java.io.{File, FileNotFoundException}
import java.util.regex.Pattern

import com.haima.sage.bigdata.etl.common.model.FileWrapper
import com.haima.sage.bigdata.etl.monitor.file.LocalFileWrapper

/**
  * Created by zhhuiyan on 15/5/20.
  */


trait PathWildMatch[T <: FileWrapper[_]] extends WildMatch {

  lazy val isWindows = System.getProperty("os.name").toLowerCase.contains("windows")

  def getFileWrapper(path: String): T

  private val split: String = "/"

  def nameWithPath(path: String): (String, String) = {
    val paths = path.split("[/]")
    if (paths.isEmpty) {
      ("", split)
    } else if (paths.length == 1) {
      (path, "." + split)
    } else if (paths.length == 2) {
      paths(0) match {
        case "" =>
          (paths(paths.length - 1), split)
        case p =>
          (paths(paths.length - 1), p)
      }
    } else {
      (paths(paths.length - 1), paths.slice(0, paths.length - 1).mkString(split))
    }
  }


  def reverseParse(path: String): (T, Array[Pattern]) = {
    val paths: Array[String] = path.split("[/]")
    var prefix: String = path
    var i: Int = paths.length
    var file = getFileWrapper(prefix)


    try {
      while ( {
        if (!file.exists && i > 0) {
          //file.close()
          true
        } else {
          false
        }
      }) {
        logger.debug(s"prefix: $prefix")
        i -= 1
        prefix =
          if (i == 0) {
            paths(0) match {
              case "" =>
                "/"
              case value =>
                value
            }
          } else {
            paths.slice(0, i).mkString(split)
          }
        file = getFileWrapper(prefix)

      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }

    logger.debug(s"end prefix: $prefix,paths:${paths.slice(i, paths.length).mkString("/")}")
    (file, paths.slice(i, paths.length).map(pattern => {
      Pattern.compile("^" + pattern + "$")
    }))


  }

  def parse(path: String): (T, Array[Pattern]) = {
    logger.debug(s"parse path $path")
    val paths: Array[String] = path.split("/")
    //logger.debug("{}",paths)
    var i: Int = 1
    var prefix: String = paths.slice(0, i).mkString(split)
    if (prefix == "") {
      prefix = "/"
    }
    //logger.debug(s"prefix: $prefix")
    var file = getFileWrapper(prefix)
    if (file.isInstanceOf[LocalFileWrapper] && isWindows) {
      var parent = file
      try {
        while (file.exists && i <= paths.length) {
          i += 1
          prefix = paths.slice(0, i).mkString(split)
          /*必须调用mkData,*/
          parent = file
          file = getFileWrapper(prefix)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }

      i -= 1
      if (i == 0) {
          throw new FileNotFoundException(s"$path not support by wildMatch ")
      }
      (parent, paths.slice(i, paths.length).map(pattern => {
        Pattern.compile("^" + pattern + "$")
      }))
    } else {
      try {
        reverseParse(path)
      } catch {
        case e:Exception=>
          throw e
      }
    }


  }


}

