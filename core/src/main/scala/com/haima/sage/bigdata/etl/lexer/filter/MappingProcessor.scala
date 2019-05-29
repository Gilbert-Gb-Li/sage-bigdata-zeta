package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Mapping
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import org.slf4j.LoggerFactory

import scala.collection.mutable


class MappingProcessor(override val filter: Mapping) extends RuleProcessor[RichMap, RichMap, Mapping] {


  private lazy val logger = LoggerFactory.getLogger(classOf[MappingProcessor])


  def mapping(data: RichMap, key: String, value: String): RichMap = {
    // logger.debug(s"mapping data [$data]")
    val rt: RichMap = data.get(key) match {
      case Some(vl) =>
        (data - key) + (value -> vl)
      case None =>
        data
    }
    rt
  }
  def handle(key:String,value:Any,pair:scala.collection.mutable.AnyRefMap[String, String]): (String,Any) ={
    if(pair.nonEmpty){
      pair.remove(key) match {
        case Some(x) =>
          (x, value)
        case _ =>
          (key, value)
      }
    }else{
      (key, value)
    }
  }
  override def process(event: RichMap): RichMap = {
    if (event.isEmpty) {
      event
    } else {
      filter.fields.foldLeft(event) {
        case (data, (key, value)) =>
          mapping(data, key, value)
        //FIXME 只能修改最外层value
        //val pair: mutable.AnyRefMap[String, String] = scala.collection.mutable.AnyRefMap[String, String](filter.fields.toSeq: _*)
        //RichMap(event.map(t=>handle(t._1,t._2,pair)))

      }
    }
  }
}