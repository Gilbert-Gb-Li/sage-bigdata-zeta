package com.haima.sage.bigdata.analyzer.dag

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._

import scala.collection.mutable

trait DAGBuilder[CONF <: Analyzer, E, T, H <: DataAnalyzer[CONF, T, T]] {

  def engine: String


  private val cacheDags: mutable.Map[String, T] = mutable.HashMap[String, T]()
  private val cacheTDags: mutable.Map[String, List[(T, Table)]] = mutable.HashMap[String, List[(T, Table)]]()

  protected final def buildDag(channel: Channel, parent: String = ""): T = {
    channel match {
      /*JOIN分析通道*/
      case TupleChannel(f, s, on: Join, _) =>
        val first = cacheDags.getOrElseUpdate(f.id.get, buildDag(f, parent + "first-"))
        val second = cacheDags.getOrElseUpdate(s.id.get, buildDag(s, parent + "second-"))
        val name = Constants.CONF.getString(s"app.$engine.analyzer.join")
        val clazz = Class.forName(name)
          .asInstanceOf[Class[JoinDataAnalyzer[T]]]
        val joinHandler = clazz.getConstructor(on.getClass).newInstance(on)
        joinHandler.join(first, second)
      case _: TableChannel =>
        throw new NotImplementedError("not support from  table channel then do none sql analyzer ")
      /*SQL分析通道*/
      case SingleChannel(channel@TupleTableChannel(f, s, _), analyzer: Option[SQLAnalyzer@unchecked], _, _, _) =>
        val sources = cacheTDags.getOrElseUpdate(channel.id.get, buildTableDag(channel, parent + "first-"))
        val handler = toTableHandler(analyzer.get)
        val environment = handler.getEnvironment(sources.head._1)
        sources.foreach(d => handler.register(d._1, d._2)(environment))
        handler.analyze(environment)

      /*Table分析通道*/
      case SingleChannel(dataSource: TableChannel, analyzer: Option[SQLAnalyzer@unchecked], _, _, _) =>
        val sources = cacheTDags.getOrElseUpdate(dataSource.id.get, buildTableDag(dataSource, parent + "first-"))
        val handler = toTableHandler(analyzer.get)
        val environment = handler.getEnvironment(sources.head._1)
        sources.foreach(d => handler.register(d._1, d._2)(environment))
        handler.analyze(environment)

      /*分析通道*/
      case SingleChannel(channel: Channel, analyzer: Option[Analyzer@unchecked], _, _, _) =>
        toDataSet(analyzer.get)(cacheDags.getOrElseUpdate(channel.id.get, buildDag(channel, parent + "first-")))
      /*wrapped 分析通道*/
      case SingleChannel(c: Channel, _, _, _, _) =>
        cacheDags.getOrElseUpdate(c.id.get, buildDag(c, parent + "first-"))
      /*解析通道*/
      case single: SingleChannel =>
        cacheDags.getOrElseUpdate(single.id.get, from(single, parent + "first-"))
    }
  }

  def clear(): Unit = {
    cacheDags.clear()
    cacheTDags.clear()
  }

  /**
    *
    * @param channel Channel 只能为 TableChannel 或者 TupleChannel(_,_,join:SQL,_)
    * @param parent
    * @return
    */
  protected final def buildTableDag(channel: TableChannel, parent: String = ""): List[(T, Table)] = {
    channel match {
      /*table join*/
      case TupleTableChannel(f, s, _) =>
        val first = cacheTDags.getOrElseUpdate(f.id.get, buildTableDag(f, parent + "first-"))
        val second = cacheTDags.getOrElseUpdate(s.id.get, buildTableDag(s, parent + "second-"))
        first ++ second
      case tableChannel: SingleTableChannel =>
        tableChannel.channel match {
          /*tuple join*/
          case tuple@TupleChannel(_, _, _: Join, id) =>
            List((cacheDags.getOrElseUpdate(id.get, buildDag(tuple, parent + "first-")), tableChannel.table))

          /*Table分析通道*/
          case SingleChannel(dataSource: TableChannel, analyzer: Option[SQLAnalyzer@unchecked], _, _, _) =>
            val sources = cacheTDags.getOrElseUpdate(dataSource.id.get, buildTableDag(dataSource, parent + "first-"))
            val handler = toTableHandler(analyzer.get)
            val environment = handler.getEnvironment(sources.head._1)
            sources.foreach(d => handler.register(d._1, d._2)(environment))
            List((handler.analyze(environment), tableChannel.table))
          /*Table分析通道*/
          case tt: TableChannel =>
            buildTableDag(tt, parent + "first-")
          /*分析通道*/
          case SingleChannel(c: Channel, analyzer@_, _, _, _) if analyzer.nonEmpty && analyzer.get.isInstanceOf[Analyzer] =>
            List((toDataSet(analyzer.asInstanceOf[Option[Analyzer]].get)(cacheDags.getOrElseUpdate(c.id.get, buildDag(c, parent + "first-"))), tableChannel.table))
          /*wrapped 分析通道*/
          case SingleChannel(c: Channel, _, _, _, _) =>
            cacheTDags.getOrElseUpdate(c.id.get, buildTableDag(SingleTableChannel(c, tableChannel.table), parent + "first-"))
          /*解析通道*/
          case c: SingleChannel =>
            List((from(c, parent + "first-"), tableChannel.table))

        }
      case _ =>
        throw new NotImplementedError("not support from  none table channel then do sql analyzer ")
    }
  }

  def sinkOrOutput(t: T): Unit

  def build(channel: Channel, analyzer: Analyzer): List[T] = {
    analyzer match {
      case sqlAnalyzer: SQLAnalyzer =>
        /*构建输出处理流*/
        val handler: TableDataAnalyzer[E, T, org.apache.flink.table.api.Table] = toTableHandler(sqlAnalyzer)
        // 使用SQLAnalyzer时: Channel 只能为 TableChannel 或者 TupleChannel(_,_,join:SQL,_)
        val sources = buildTableDag(channel.asInstanceOf[TableChannel])
        implicit val environment: E = handler.getEnvironment(sources.head._1)
        sources.foreach(d => handler.register(d._1, d._2)(environment))

        handler.execute(environment)

      case _ =>
        /*构建输出处理流*/
        buildNotSQL(channel, analyzer)
    }
  }

  /**
    * 对于非SQL的分析 构建处理流
    *
    * @param channel
    * @param analyzer
    * @return
    */
  def buildNotSQL(channel: Channel, analyzer: Analyzer): List[T] = {
    /*构建输出处理流*/
    val handler: H = toHandler(analyzer)
    handler.handle(buildDag(channel))
  }

  def buildWithOut(channel: Channel, analyzer: Analyzer): Unit = {
    build(channel, analyzer).foreach(sinkOrOutput)
    cacheTDags.clear()
    cacheDags.clear()
  }

  def from(channel: SingleChannel, path: String): T

  def toDataSet(analyzer: Analyzer)(implicit before: T): T = {
    toHandler(analyzer).action(before)
  }

  /*构建处理器*/
  def toHandler(analyzer: Analyzer): H

  /*构建TABLE处理器*/
  def toTableHandler(sqlAnalyzer: SQLAnalyzer): TableDataAnalyzer[E, T, org.apache.flink.table.api.Table]

}
