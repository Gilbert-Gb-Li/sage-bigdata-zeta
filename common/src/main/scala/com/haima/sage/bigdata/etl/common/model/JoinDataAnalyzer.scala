package com.haima.sage.bigdata.etl.common.model

import java.util
import javax.script.{Compilable, ScriptEngine, ScriptEngineManager, SimpleBindings}

import jdk.nashorn.api.scripting.NashornScriptEngineFactory

import scala.util.Try

/**
  * Created by zhhuiyan on 2017/4/25.
  */
trait JoinDataAnalyzer[T] extends Serializable {

  import scala.collection.JavaConversions._

  private lazy val manager = new ScriptEngineManager
  private lazy val scriptEngine: ScriptEngine = {
    val factory = manager.getEngineFactories.find(_.getEngineName == "Oracle Nashorn").get.asInstanceOf[NashornScriptEngineFactory]
    factory.getScriptEngine("-doe", "--global-per-engine")

  }

  private lazy val compiled = scriptEngine.asInstanceOf[Compilable].compile(
    s"""function exec(first,second){
       |  ${conf.operation.get}
       |};
       |exec(first,second);""".stripMargin)

  //执行数据合并操作,么有配置script时,只执行map++map
  final def exec: (RichMap, RichMap) => RichMap =
    conf.operation match {
      case Some(script: String) if script != null =>
        (f, s) =>
          val data = Try {
            val first = new util.HashMap[String, Any]()
            first.putAll(f)
            val second = new util.HashMap[String, Any]()
            second.putAll(s)
            val bindings = new SimpleBindings
            bindings.put("first", first)
            bindings.put("first", second)
            compiled.eval(bindings).asInstanceOf[java.util.Map[String, Any]].toMap

          }.transform(Try(_), e => {
            Try(Map("error" -> e.getMessage))
          }).get
          RichMap(data)

      case _ =>
        (f, s) =>
          f.++(s)
    }


  def engine(): String

  def conf: Join

  /*private lazy val name = Constants.CONF.getString(s"app.${conf.model.getOrElse(AnalyzerModel.STREAMING)}.analyzer.${conf.analyzer.name}")

  /*构建输出处理流*/
  private lazy val handler: DataAnalyzer[_ <: Analyzer, T, T] = {
    if (name != null) {
      val clazz = Class.forName(name).asInstanceOf[Class[DataAnalyzer[_ <: Analyzer, T, T]]]
      clazz.getConstructor(conf.analyzer.getClass).newInstance(conf.analyzer)
    } else {
      logger.error(s"unknown analyzer for FlinkStream process :${conf.analyzer} ")
      throw new UnsupportedOperationException(s"unknown analyzer for FlinkStream process :${conf.analyzer.getClass} .")
    }
  }*/

  @throws[Exception]
  def join(first: T, second: T): T

}
