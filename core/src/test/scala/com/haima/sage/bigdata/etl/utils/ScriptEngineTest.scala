package com.haima.sage.bigdata.etl.utils


import java.util
import javax.script.{Compilable, ScriptEngineManager, ScriptException}

import jdk.nashorn.api.scripting.NashornScriptEngineFactory
import org.junit.Test

import scala.collection.JavaConversions._
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by zhhuiyan on 2017/3/22.
  */
class ScriptEngineTest {
  val manager = new ScriptEngineManager
  val engine = {
    val factory = manager.getEngineFactories.find(_.getEngineName == "Oracle Nashorn").get.asInstanceOf[NashornScriptEngineFactory]
    factory.getScriptEngine("-doe", "--global-per-engine")
  }

  @Test
  def engineName(): Unit = {
    import javax.script.ScriptEngineManager
    val sm = new ScriptEngineManager

    for (f <- sm.getEngineFactories) {

      println(f.getNames)
      f.getNames.foreach(name => {
        println(sm.getEngineByName(name))
      })

    }
  }


  @Test
  def evalDate() {

    (0 to 1).foreach(i => {
      val event = new util.HashMap[String, String]()
      event.put("vc_begintime", "20180306053848")
      event.put("vc_endtime", "20180306053841")
      val script =
        """function exec(event){
          |var vc_begintime = event.get("vc_begintime");
          |var vc_endtime = event.get("vc_endtime");
          |var dd= Date.parse(new Date(vc_begintime.substr(0,4)+"-"+vc_begintime.substr(4,2)+"-"+vc_begintime.substr(6,2)+" "+vc_begintime.substr(8,2)+":"+vc_begintime.substr(10,2)+":"+vc_begintime.substr(12,2)));
          |event.put("usedtime", dd)
          |return event;
          |};
          | exec(event);
          |""".stripMargin
      /*
      *
        */
      import javax.script.SimpleBindings
      val bindings = new SimpleBindings
      bindings.put("event", event)
      val rt = engine.asInstanceOf[Compilable].compile(script).eval(bindings)
      println(rt)
      println(rt.asInstanceOf[util.HashMap[String, String]].get("usedtime").getClass)
      Thread.sleep(10)
    })

  }

  @Test
  def exec2(): Unit = {

    /* val settings = engine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
     settings.usejavacp.value = true*/
    val source: Map[String, String] = Map("src" -> "127.0.0.1", "aaa" -> "bbbb")

    val event = new util.HashMap[String, String]()

    source.foreach(entry => event.put(entry._1, entry._2))
    engine.put("b", 3)
    engine.put("a", 1)
    engine.put("b", 3)
    engine.put("a", 1)

    engine.put("source", event)
    try { // 只能为Double，使用Float和Integer会抛出异常
      val rt = engine.eval(
        s"""source.get('src1')==null """.stripMargin)
      println("s:" + rt.getClass + ":-:" + rt)
    } catch {
      case e: ScriptException =>
        e.printStackTrace()
    }
  }


  @Test
  def exec(): Unit = {
    import scala.collection.JavaConversions._
    val manager = new ScriptEngineManager
    val engine = manager.getEngineByName("javascript")

    /* val settings = engine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
     settings.usejavacp.value = true*/
    val source: Map[String, String] = Map("src" -> "127.0.0.1", "aaa" -> "bbbb")

    val event = new util.HashMap[String, String]()

    source.foreach(entry => event.put(entry._1, entry._2))
    engine.put("b", 3)
    engine.put("a", 1)
    engine.put("b", 3)
    engine.put("a", 1)

    engine.put("source", event)
    try { // 只能为Double，使用Float和Integer会抛出异常
      val rt = engine.eval(
        s"""data =new Date();
           | source.put("timestamp",data.getFullYear()+"-"+( data.getMonth()+1)+"-"+data.getDate()+" "+data.getHours()+":"+data.getMinutes()+":"+data.getSeconds() );
           |source.remove("aaa")
           |
           |
           |source""".stripMargin)
      val result = engine.get("source").asInstanceOf[util.Map[String, Any]].toMap
      System.out.println("rt = " + rt)
      System.out.println("result = " + result)
      val date = result.get("timestamp")
      System.out.println("date  = " + date + "type:" + date.get.getClass)
      System.out.println("result = " + rt)
      engine.eval("c=a+b")
      val c = engine.get("c").asInstanceOf[Double]
      System.out.println("c = " + c)
    } catch {
      case e: ScriptException =>
        e.printStackTrace()
    }
  }

  @Test
  def priorityFunc(): Unit = {
    val manager = new ScriptEngineManager
    val engine = manager.getEngineByName("javascript")
    //event
    val event = Map[String, Any](("code", "040"))
    val source = new util.HashMap[String, Any]()
    event.foreach(entry => source.put(entry._1, entry._2))
    implicit val ord: Ordering[(Map[String, Any], Int, Long)] = Ordering.by {
      data =>
        (data._2 + 1) * data._3
    }
    val cache: mutable.PriorityQueue[(Map[String, Any], Int, Long)] = new mutable.PriorityQueue[(Map[String, Any], Int, Long)]()
    var map1 = Map[String, Any](("code", "040"), ("note", "China"))
    var map2 = Map[String, Any](("code", "200"), ("note", "Japan"))
    var map3 = Map[String, Any](("code", "300"), ("note", "Russia"))
    cache.enqueue((map1, 0, 10000003))
    cache.enqueue((map2, 0, 10000002))
    cache.enqueue((map3, 0, 10000001))

    engine.put("event", source)
    engine.put("cache", cache)
    engine.get("cache")
    println(cache)
    val rs = engine.get("event")
    println(rs)
    println("")

    val r1 = engine.eval("cache.length()")
    println("r1: " + r1)
    val r2 = engine.eval("cache.isEmpty()")
    println("r2: " + r2)
    val r3 = engine.eval("cache.toList()")
    println("r3: " + r3)

    try {
      var r4 = engine.eval(
        """
           var list=cache.toList();
           for (var i=0;i<list.length();i++){
             var obj=list.apply(i);
             if(obj._1().get("code").get()==event.get("code")){
                event.put("a"+i,obj._1().get("note").get())
             }
          }
          event
        """.stripMargin)
      println("r4: " + engine.get("event"))
    }
    catch {
      case e: ScriptException =>
        e.printStackTrace()
    }


  }

}
