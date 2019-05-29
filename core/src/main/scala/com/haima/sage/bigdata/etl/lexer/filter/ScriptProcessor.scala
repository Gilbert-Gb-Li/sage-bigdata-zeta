package com.haima.sage.bigdata.etl.lexer.filter

import java.util

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Script
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import javax.script.{Compilable, ScriptEngine, ScriptEngineManager, SimpleBindings}
import jdk.nashorn.api.scripting.NashornScriptEngineFactory

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.util.Try

class ScriptProcessor(override val filter: Script) extends RuleProcessor[RichMap, RichMap, Script] {


  import scala.collection.JavaConversions._


  private lazy val exec: RichMap => RichMap = if (filter.`type` == "scala") {
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
           |import com.haima.sage.bigdata.etl.utils.RegexFindUtils._
           |import com.haima.sage.bigdata.etl.utils.NumberUtils._
           |import com.haima.sage.bigdata.etl.utils.XmlToTextUtils._
           |import com.haima.sage.bigdata.etl.utils.XmlXpathUtils._
           |val exec:RichMap=>RichMap=event=>{
           |${filter.script};
           |}
           |exec
           |""".stripMargin).eval().asInstanceOf[RichMap => RichMap]
    } finally {
      engine.close()
    }

  } else {
    val engine: ScriptEngine = {
      val factory = new ScriptEngineManager().getEngineFactories.find(_.getEngineName == "Oracle Nashorn").get.asInstanceOf[NashornScriptEngineFactory]
      factory.getScriptEngine("-doe", "--global-per-engine")
    }
    val compiled = engine.asInstanceOf[Compilable].compile(
      s"""function exec(event){
         |  ${filter.script}
         |  return event;
         |};
         |exec(event);""".stripMargin)
    val bindings = new SimpleBindings

    event: RichMap => {
      val data = Try {
        val source = new util.HashMap[String, Any]()
        source.putAll(event)

        bindings.put("event", source)
        val rt = compiled.eval(bindings).asInstanceOf[java.util.Map[String, Any]].toMap
        bindings.clear()
        rt
      }.transform(Try(_), e => {
        Try(event + ("c@error" -> e.getMessage))
      }).get
      data
    }

  }


  override def process(event: RichMap): RichMap = {
    try {
      exec(event)
    }catch {
      case e:Exception=>
        event +("c@error"->s"${filter.`type`}: ${e.getMessage}")
    }


  }

}