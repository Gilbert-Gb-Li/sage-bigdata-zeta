package com.haima.sage.bigdata.analyzer.modeling.previewer

import java.util.UUID

import akka.actor.{Actor, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.analyzer.dag.DAGBuilder
import com.haima.sage.bigdata.analyzer.modeling.DataModelingAnalyzer
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.preview.PreviewServer
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await

/**
  * Created by ChengJi on 2017/11/8.
  */
class ModelingPreviewer extends Actor with DAGBuilder[Analyzer, BatchTableEnvironment, DataSet[RichMap], DataModelingAnalyzer[Analyzer]] {

  implicit lazy val previewTimeout = Timeout(10, java.util.concurrent.TimeUnit.SECONDS)

  private val env = ExecutionEnvironment.getExecutionEnvironment

  def logger: Logger = LoggerFactory.getLogger("ModelingPreviewer")


  override def receive: Receive = {
    case (channel: Channel, analyzer: Analyzer) =>
      sender() ! preview(channel, analyzer)
      context.stop(self)
    case Opt.STOP =>
      context.stop(self)
    case msg =>
      logger.warn(s"ignore message:$msg,sender is ${sender().path}")
  }

  def preview(channel: Channel, analyzer: Analyzer): List[RichMap] = {
    try {
      build(channel, analyzer).map(_.collect()).reduce(_ ++ _).toList
    } catch {
      case e: Exception =>
        e.printStackTrace()
        context.stop(self)
        List(Map("error" -> (e.getCause + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))))
    }
  }

  override def engine: String = "modeling"

  override def sinkOrOutput(t: DataSet[RichMap]): Unit = {
    throw new NotImplementedError("preview need  to out put ")
  }

  override def from(channel: SingleChannel, path: String): DataSet[RichMap] = {

    val previewServer = context.actorOf(Props.create(classOf[PreviewServer]), s"preview-server-${UUID.randomUUID()}")


    val data: List[RichMap] = try {
      Await.result(previewServer ? (channel.dataSource, channel.parser.get), previewTimeout.duration).asInstanceOf[List[RichMap]] match {
        case Nil =>
          previewServer ! Opt.STOP
          List(Map("error" -> s"Can not preview,please check whether the datasource you have configured exists"))
        case cache: List[RichMap] =>
          cache
      }
    } catch {
      case _: Exception =>
        previewServer ! Opt.STOP
        List(Map("error" -> s"Can not preview,please check whether the datasource you have configured exists"))

    }
    //    previewServer ! Opt.STOP
    env.fromElements(data: _*)
  }

  override def toHandler(analyzer: Analyzer): DataModelingAnalyzer[Analyzer] = {
    val name: String = Constants.CONF.getString(s"app.modeling.analyzer.${analyzer.name}")
    if (name != null) {
      val clazz = Class.forName(name).asInstanceOf[Class[DataModelingAnalyzer[Analyzer]]]
      clazz.getConstructor(analyzer.getClass, classOf[AnalyzerType.Type]).newInstance(analyzer, AnalyzerType.ANALYZER)
    } else {
      logger.error(s"unknown analyzer for FlinkStream process :$analyzer ")
      throw new UnsupportedOperationException(s"unknown analyzer for FlinkStream process :${analyzer.getClass} .")
    }
  }

  override def toTableHandler(sqlAnalyzer: SQLAnalyzer): TableDataAnalyzer[BatchTableEnvironment, DataSet[RichMap], org.apache.flink.table.api.Table] = {
    {
      /*val clazz = Class.forName(name).asInstanceOf[Class[DataModelingAnalyzer[_ <: Analyzer]]]
                  clazz.getConstructor(d.getClass).newInstance(d)*/
      /*构建输出处理流*/
      val name: String = Constants.CONF.getString(s"app.modeling.analyzer.${sqlAnalyzer.name}")
      if (name != null) {
        val clazz = Class.forName(name).asInstanceOf[Class[TableDataAnalyzer[BatchTableEnvironment, DataSet[RichMap], org.apache.flink.table.api.Table]]]
        clazz.getConstructor(sqlAnalyzer.getClass, classOf[AnalyzerType.Type]).newInstance(sqlAnalyzer,
          AnalyzerType.ANALYZER)
      } else {
        logger.error(s"unknown analyzer for FlinkStream process :$sqlAnalyzer ")
        throw new UnsupportedOperationException(s"unknown analyzer for FlinkStream process :${sqlAnalyzer.getClass} .")
      }

    }
  }
}
