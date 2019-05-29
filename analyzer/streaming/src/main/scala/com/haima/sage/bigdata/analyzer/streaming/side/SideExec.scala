package com.haima.sage.bigdata.analyzer.streaming.side

import com.haima.sage.bigdata.etl.common.model._
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable

trait SideExec[ENV] {
  def exec(sql: String, tables: Map[Table, DataSource], registeredTable: Map[String, (org.apache.flink.table.api.Table, Option[DataStream[RichMap]])], tableEnv: ENV): Option[String]

  def analyzeSide(sql: String, channel: TableChannel, registeredTable: Map[String, (org.apache.flink.table.api.Table, Option[DataStream[RichMap]])], tableEnv: ENV): Option[String] = {
    exec(sql, parseTables(channel), registeredTable, tableEnv)
  }

  /**
    * 获取转表的DataSource信息
    *
    * @param channel
    * @return Map(TableName -> DataSource)
    */
  def parseTables(channel: TableChannel): Map[Table, DataSource] = {
    val sources: mutable.Map[Table, DataSource] = mutable.Map()
    findParserChannel(channel)

    def findParserChannel(c: Channel): Unit = {
      c match {
        //解析通道
        case SingleTableChannel(SingleChannel(ds, _, _,_, _), table, _) =>
          sources.put(table, ds)
        case TupleTableChannel(f, s, _) =>
          findParserChannel(f)
          findParserChannel(s)
        case _ =>
      }

    }

    sources.toMap
  }
}
