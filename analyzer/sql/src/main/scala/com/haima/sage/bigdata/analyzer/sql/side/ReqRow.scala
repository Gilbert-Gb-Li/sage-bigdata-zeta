package com.haima.sage.bigdata.analyzer.sql.side

import com.haima.sage.bigdata.etl.common.model.{DataSource, SideTable}
import org.apache.flink.types.Row

trait ReqRow {

  val dataSource: DataSource
  val table: SideTable
  val sideInfo: SideInfo

  def initCache()

  def fillData(input: Row, sideInput: Any): Row
}
