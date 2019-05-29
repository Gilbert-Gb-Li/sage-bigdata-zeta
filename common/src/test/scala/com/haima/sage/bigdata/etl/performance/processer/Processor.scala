package com.haima.sage.bigdata.etl.performance.processer

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable

abstract class Processor(event: RichMap){
  var result = new mutable.HashMap[String, Any]()
  result ++= event
  @throws(classOf[Exception])
  def run(): Unit
}

object Processor {
  var result: mutable.HashMap[String, Any] = _
  def run(p: Processor): RichMap = {
    try{
      result = p.result
      p.run()
      RichMap(result.toMap)
    }catch {
      case ex: Exception =>
        result += ("meta_table_name" -> "error")
        result += ("reason" -> ex.getMessage)
        RichMap(result.toMap)
    }

  }

  def replaceProcess(index: String, replacements: Map[String, String]): Unit ={
    var value = result.getOrElse(index, "").toString.trim
      for (r <- replacements) {
        value = value.replace(r._1, r._2)
      }
    result += (index -> value)
  }

  def replaceProcess(replacements: Seq[(String, Map[String, String])]): Unit ={
    for (r <- replacements) {
      replaceProcess(r._1, r._2)
    }
  }

  def typeProcess(items: Map[String, String]): Unit ={
    for (item <- items) {
      item._2.toLowerCase match {
        case "int" => result += (item._1 -> result.getOrElse(item._1, 0).toString.toInt)
        case "long" => result += (item._1 -> result.getOrElse(item._1, 0L).toString.toLong)
        case _ =>
      }
    }
  }

  def regexProcess(reg: Seq[(String, String, String)]): Unit ={
    for (r <- reg) {
      val p = Pattern.compile(r._2)
      val m = p.matcher(result.getOrElse(r._1, "").toString)
      val fs = r._3.split(":")
      if(m.find) {
        for (i <- 1 to fs.length){
          result += (fs(i-1).trim -> m.group(i))
        }
      }
    }
  }

  def defaultProcess(defaults: Map[String, Any]): Unit ={
    for(d <- defaults) result.getOrElseUpdate(d._1, d._2)
  }

  def userDefineProcess(index: String, f: () => Any): Unit = {
    result += (index -> f())
  }

}


object Main extends App {
  val event = RichMap(Map("popularity" -> "--", "nickname" -> "沁墨墨是大总攻  UP7",
    "guard_num"->"大航海(120)","timestamp"->"2019-05-24 00:02:11"))
Processor.run(new Processor(event) {
  val tsp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  override def run(): Unit = {
    Processor.userDefineProcess("timestamp", ()=> {
      event.get("timestamp") match {
        case Some(x) => tsp.parse(x.toString).getTime
        case None => System.currentTimeMillis()
      }
    })
    Processor.userDefineProcess("guard_num",()=>{
      val p = Pattern.compile("大航海[\\(|\\s+]*(\\d+).*")
      val m = p.matcher(event.getOrElse("guard_num", "").toString)
      if (m.find) m.group(1) else 0
    })
  }
})
    .foreach(x=> println(x._1,x._2))



}


