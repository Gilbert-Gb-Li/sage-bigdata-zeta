package com.haima.sage.bigdata.analyzer.utils

import java.io.File


object JarUtils {

  class JarUtils

  /* 使用的jar包 */
  def jar: List[String] = {
    val path = classOf[JarUtils].getClass.getResource("/").getPath
    val paths = path.split("/")

    (if (path.endsWith("classes/")) {
      val parent = paths.slice(0, paths.length - 3).mkString("/") + "/"
      new File(parent + "analyzer")
        .listFiles().filter(_.isDirectory)
        .map(model => {

          model.listFiles().find(_.getName == "target").map(_.listFiles().filter(file => {
            file.getName.startsWith("sage-bigdata-zeta-analyzer") && file.getName.endsWith(".jar")
          }).head

          )
        }
        ).filter(_.isDefined).map(_.get)

    } else {
      new File(paths.slice(0, paths.length - 1).mkString("/") + "/lib").listFiles().filter(
        file => file.getName.startsWith("sage-bigdata-zeta-analyzer") &&
          file.getName.endsWith(".jar")
      )
    }).map(_.getAbsolutePath).toList
  }

}
