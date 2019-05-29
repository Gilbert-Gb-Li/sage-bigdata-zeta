package com.haima.sage.bigdata.analyzer.aggregation.streaming

import com.haima.sage.bigdata.analyzer.aggregation.model.{Pattern, Terms}
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{LogReduceAnalyzer, RichMap}
import com.haima.sage.bigdata.etl.streaming.flink.analyzer.{DataStreamAnalyzer, SimpleDataStreamAnalyzer}

import scala.collection.mutable


/**
  * Created by wxn on 17/11/01.
  *
  */
class StreamLogReducerAnalyzer(override val conf: LogReduceAnalyzer) extends SimpleDataStreamAnalyzer[LogReduceAnalyzer, List[(Pattern, String)]] {


  override def convert(model: Iterable[Map[String, Any]]): List[(Pattern, String)] = {
    model.map(d => {
      (Pattern(d("pattern").toString), d("pattern_key").toString)
    }).toList.sortWith((f, s) => f._1.length > s._1.length)

  }

  override def analyzing(in: RichMap, models: List[(Pattern, String)]): RichMap = {
    if(models==null || models.isEmpty){
      throw new ExceptionInInitializerError("日志归并需要提前建模才能执行！")
    }

    in.get(conf.field.get) match {
      case Some(v) =>
        models.find(m => {
          m._1.isMatch(Terms(v.toString.trim))
        }) match {
          case Some((_, key)) =>
            in + ("isMatch" -> true, "patternKey" -> key)
          case _ =>
            in + ("isMatch" -> false)
        }
      case _ =>
        in + ("isMatch" -> false)
    }
  }
}
