package com.haima.sage.bigdata.etl.common.model.filter

import java.util

import com.haima.sage.bigdata.etl.common.exception.LogParserException
import com.haima.sage.bigdata.etl.common.model.RichMap
import javax.script.{Compilable, ScriptEngine, ScriptEngineManager, SimpleBindings}
import jdk.nashorn.api.scripting.NashornScriptEngineFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

trait ScriptFilter[R <: Rule, C <: Case[R]] extends SwitchRule[R, C] {
  private lazy val logger: Logger = LoggerFactory.getLogger("com.haima.sage.bigdata.etl.common.model.filter.ScriptFilter")

  import scala.collection.JavaConversions._


  def script: String

  def `type`: String = "scala" //options scala js


  private lazy val exec: RichMap => Any = if (`type` == "scala") {
    val engine = {
      val settings = new Settings()
      settings.usemanifestcp.value = true
      settings.usejavacp.value = true
      new IMain(null, settings)
    }
    try {
      engine.asInstanceOf[Compilable].compile(
        s"""
           |import com.haima.sage.bigdata.etl.common.model.RichMap
           |val exec:RichMap=>Any=event=>{
           |$script;
           |}
           |exec
           |""".stripMargin).eval().asInstanceOf[RichMap => Any]
    } finally {
      engine.close()
    }

  } else {
    val engine: ScriptEngine = {
      val manager = new ScriptEngineManager
      val factory = manager.getEngineFactories.find(_.getEngineName == "Oracle Nashorn").get.asInstanceOf[NashornScriptEngineFactory]
      factory.getScriptEngine("-doe", "--global-per-engine")

    }
    val compiled = engine.asInstanceOf[Compilable].compile(
      s"""$script""".stripMargin)
    val bindings = new SimpleBindings

    event: RichMap => {
      val source = new util.HashMap[String, Any]()
      source.putAll(event)
      bindings.put("event", source)
      compiled.eval(bindings)
    }

  }

  override def get(event: RichMap): Option[Any] = {
    Option(
      try {
        exec(event)
      } catch {
        case e: Exception =>
          throw new LogParserException(s"${`type`}: get value error ${e.getMessage}")
      })
  }


  override def filterOne(value: String)(test: C): Boolean = {
    test.value.equals(value)


  }
}

