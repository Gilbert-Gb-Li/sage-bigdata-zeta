package com.haima.sage.bigdata.etl.performance.utils

import java.io.File

import com.haima.sage.bigdata.etl.common.model.RichMap
import javax.script.Compilable

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

object ScriptEngineUtils {

  def process(event: RichMap, script: String, times: Int = 1): Unit = {
    val engine = {
      val settings = new Settings()
      settings.usemanifestcp.value = true
      settings.usejavacp.value = true
      new IMain(null, settings)
    }
    try {
      println(script)
      val exec = genExec(engine, script)
      val start = System.currentTimeMillis()
      val count = times
      var res: Any = null
      for (_ <- 1 to count) {
        try {
          res = exec(event)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
      val l2 = System.currentTimeMillis()
      println(s"performance:${times}", (l2 - start), ((l2 - start).toDouble / count))
      (res.asInstanceOf[Map[String, Any]] - "raw" - "c@raw" - "data").toList.foreach(println)
    } finally {
      engine.close()
    }
  }

  private def genExec(engine: IMain, script: String) = {
    engine.asInstanceOf[Compilable].compile(
      s"""
         |import com.haima.sage.bigdata.etl.common.model.RichMap
         |val exec:RichMap=>Any=event=>{
         |$script;
         |}
         |exec
         |""".stripMargin).eval().asInstanceOf[RichMap => Any]
  }

  def genScript(files: Seq[String], invoke: String): String = {

    var script: String = ""
    if (files != null && files.nonEmpty) {
      files.foreach(path => {
        val file = new File(path)
        script += TestUtils.readIgnoreFirst(file)
      })
    }


    script + invoke
  }
}
