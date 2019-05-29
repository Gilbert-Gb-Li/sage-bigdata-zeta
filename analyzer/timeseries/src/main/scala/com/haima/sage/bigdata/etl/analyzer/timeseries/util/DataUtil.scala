package com.haima.sage.bigdata.analyzer.timeseries.util


import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.analyzer.timeseries.models.{ARIMAModel, HoltWintersModel, TimeSeriesModel}
import com.haima.sage.bigdata.etl.common.model.{ARIMA, HoltWinters, HoltWintersType, RichMap}
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * Created by CaoYong on 2018/1/4.
  */
class DataUtil extends Serializable {


  //根据知识库Id，返回模型
  def getTimeSeriesModel( data: List[Map[String, Any]],name:Option[String]): List[(String, Map[String, Any], TimeSeriesModel)] = {
    val mapper = new Mapper() {}.mapper
    data.map(d =>
      (d("field").toString,
        mapper.readValue[Map[String, Any]](d("groups").toString),
        mapper.readValue[TimeSeriesModel](d("model").toString))
    ).toList

  }

  def trainfilter: RichFilterFunction[(Long, RichMap)] = new RichFilterFunction[(Long, RichMap)] {
    var total_count: Long = _

    override def open(parameters: Configuration): Unit = {
      total_count = getRuntimeContext.getBroadcastVariable[Long]("total_count").get(0)
    }

    override def filter(v: (Long, RichMap)): Boolean = {
      v._1 <= total_count / 2
    }
  }

  def testfilter: RichFilterFunction[(Long, RichMap)] = new RichFilterFunction[(Long, RichMap)] {
    var total_count: Long = _

    override def open(parameters: Configuration): Unit = {
      total_count = getRuntimeContext.getBroadcastVariable[Long]("total_count").get(0)
    }

    override def filter(v: (Long, RichMap)): Boolean = {
      v._1 > total_count / 2
    }
  }
  //时间戳转换为时间
  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }

}
