package com.haima.sage.bigdata.etl.preview

import akka.actor.ActorRef
import com.haima.sage.bigdata.etl.common.model.writer.{ContentType, Json}
import com.haima.sage.bigdata.etl.writer.Formatter

/**
  * Created by jdj on 2017/8/29.
  * 存储预览工具类
  */
object PreviewWriter{


  def preview(t: Map[String, Any],contentType:Option[ContentType]):String = {
    val mapper = Formatter(contentType)
    val line = mapper.string(t)
    line
  }

  def main(arg:Array[String]): Unit ={
    var map = Map[String,String]()
    map = map.+("a"->"1")
    val json = Json(Option("GBK"))
    println(preview(map,Option(json)))
  }

}
