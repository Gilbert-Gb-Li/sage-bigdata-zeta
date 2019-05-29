package com.haima.sage.bigdata.analyzer.modeling.reader

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.haima.sage.bigdata.etl.common.model.filter.{Filter, MapRule}
import com.haima.sage.bigdata.etl.common.model.{Parser, RichMap, SingleChannel}
import com.haima.sage.bigdata.etl.lexer.filter.RuleProcessorFactory
import com.haima.sage.bigdata.etl.normalization.Normalizer
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * Created by evan on 17-8-30.
  *
  * Modeling readers
  *
  */
trait FlinkModelingReader extends Normalizer {
  private lazy val emptyFilters = new Array[SerializeFilter](0)


  override def toJson(value: Any): String = JSON.toJSONString(value, Seq(): _*)

  private val LOG = LoggerFactory.getLogger(classOf[FlinkModelingReader].getName)

  def channel: SingleChannel

  def getDataSet(evn: ExecutionEnvironment): DataSet[RichMap]

  override def parser: Parser[MapRule] = channel.parser.get.asInstanceOf[Parser[MapRule]]

  protected lazy val filter: Option[Filter] = channel.parser.map(_.filter) match {
    case Some(f) if f != null && f.nonEmpty =>
      Some(Filter(f.asInstanceOf[Array[MapRule]]))
    case _ =>
      None
  }

  def execute(evn: ExecutionEnvironment): DataSet[RichMap] = {
    getDataSet(evn).flatMap {
      obj =>
        filter.map(f => f.filter(RuleProcessorFactory.getProcessors(f), List(obj))).getOrElse(List(obj)).map(normalize)
    }(createTypeInformation[RichMap], ClassTag(classOf[RichMap]))
  }

}
