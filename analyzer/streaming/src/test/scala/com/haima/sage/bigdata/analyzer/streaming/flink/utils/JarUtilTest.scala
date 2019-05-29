package com.haima.sage.bigdata.analyzer.streaming.flink.utils

import java.io.File

import com.haima.sage.bigdata.analyzer.streaming.lexer.FlinkLexer
import org.junit.Test

class JarUtilTest {

  @Test
  def read(): Unit = {
    val jar: List[String] = {
      val path = classOf[FlinkLexer].getClass.getResource("/").getPath
      val paths = path.split("/")

      (if (path.endsWith("classes/")) {
        val parent = paths.slice(0, paths.length - 3).mkString("/") + "/"
        new File(parent + "streaming/target").listFiles()
          .filter(file => file.getName.startsWith("streaming") && file.getName.endsWith(".jar"))
          .++(
            new File(parent + "analyzer")
              .listFiles().filter(_.isDirectory)
              .map(_.listFiles().filter(_.getName == "target")
                .map(_.listFiles().filter(file =>
                  file.getName.startsWith("analyzer") && file.getName.endsWith(".jar")).head).head).filter(_ != null))

      } else {
        new File(paths.slice(0, paths.length - 1).mkString("/") + "/lib").listFiles().filter(file => file.getName.startsWith("streaming") && file.getName.endsWith(".jar"))
      }).map(_.getAbsolutePath).toList
      //dir.listFiles().foreach(name => logger.info(s"  jar in path  :$name"))
      //dir.listFiles().filter(file => file.getName.startsWith("streaming") && file.getName.endsWith(".jar")).map(_.getAbsolutePath).headOption

    }
  }
}
