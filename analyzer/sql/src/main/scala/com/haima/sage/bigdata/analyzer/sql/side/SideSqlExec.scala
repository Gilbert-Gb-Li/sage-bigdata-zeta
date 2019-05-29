package com.haima.sage.bigdata.analyzer.sql.side

import java.sql
import java.sql.{Time, Timestamp}
import java.util.Date
import java.util.concurrent.TimeUnit

import com.dtstack.flink.sql.side._
import com.haima.sage.bigdata.analyzer.sql.utils.TableUtils
import com.haima.sage.bigdata.analyzer.streaming.side.SideExec
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{DataSource, RichMap}
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.SqlNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.calcite.shaded.com.google.common.collect.HashBasedTable
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.types.Row

import scala.collection.mutable

class SideSqlExec extends com.dtstack.flink.sql.side.SideSqlExec with SideExec[StreamTableEnvironment] with Logger with Serializable {

  private val sideSQLParser = new SideSQLParser

  def exec(sql: String, tables: Map[com.haima.sage.bigdata.etl.common.model.Table, DataSource], registeredTable: Map[String, (Table, Option[DataStream[RichMap]])], tableEnv: StreamTableEnvironment): Option[String] = {
    logger.info(s"start parser side table, sql = $sql")
    import scala.collection.JavaConversions._
    // 维表信息
    val sideTables = tables.filter(_._1.isSideTable).map {
      case (t, _) =>
        (t.tableName.toUpperCase, t)
    }
    // 非维表信息
    val noSideTables = tables.filter(!_._1.isSideTable).map {
      case (t, _) =>
        (t.tableName.toUpperCase, t)
    }
    // 解析SQL后的分解任务队列
    val exeQueue = sideSQLParser.getExeQueue(sql, sideTables.keys.toList)

    // 前一个任务是否是 维表Join
    var preIsSideJoin = false
    // Join 后的新表信息
    val replaceInfoList = mutable.ListBuffer[FieldReplaceInfo]()
    // 缓存注册后的表信息（包括非维表，以及维表Join）
    val localTableCache = mutable.Map[String, (Table, Option[DataStream[RichMap]])]()
    localTableCache.putAll(registeredTable)
    // 最后执行的SQL
    var fSql: Option[String] = None

    var pollObj: Object = exeQueue.poll()
    while (pollObj != null) {

      pollObj match {
        case sqlNode: SqlNode =>
          if (preIsSideJoin) {
            preIsSideJoin = false
            replaceInfoList.foreach {
              replaceInfo =>
                replaceFieldName(sqlNode, replaceInfo.getMappingTable, replaceInfo.getTargetTableName, replaceInfo.getTargetTableAlias)
            }
            sqlNode.getKind match {
              case INSERT =>
                tableEnv.sqlUpdate(sqlNode.toString)
              case AS =>
                val aliasInfo = parseASNode(sqlNode)
                val table = tableEnv.sqlQuery(aliasInfo.getName)
                tableEnv.registerTable(aliasInfo.getAlias, table)
                localTableCache.put(aliasInfo.getAlias, (table, None))
              case SELECT =>
                fSql = Some(sqlNode.toString)
              case _ =>
            }
          }
        case joinInfo: JoinInfo =>
          preIsSideJoin = true

          val joinScope = new JoinScope

          // 设置Join 的Left table 信息
          val leftScopeChild = new JoinScope.ScopeChild
          // 别名
          leftScopeChild.setAlias(joinInfo.getLeftTableAlias)
          // 表名称
          leftScopeChild.setTableName(joinInfo.getLeftTableName)

          // 获取左表的信息
          val targetTable = localTableCache.get(joinInfo.getLeftTableAlias) match {
            case Some(t) => t
            case None => localTableCache(joinInfo.getLeftTableName)
          }
          // 左表转成DataStream[Row]
          /*val adaptStream = targetTable._2 match {
            case Some(ds) => ds.map(map2Row(leftFieldWithTypes))(leftScopeChild.getRowTypeInfo)
            case None => tableEnv.toAppendStream[Row](targetTable._1)(leftScopeChild.getRowTypeInfo)
          }*/
          val inputStream = targetTable._2 match {
            case Some(ds) =>
              // the TypeInfo of Join Left table
              val leftFieldWithTypes = TableUtils.toTypeInfo(noSideTables.get(joinInfo.getLeftTableAlias) match {
                case Some(t) => t
                case None => noSideTables(joinInfo.getLeftTableName)
              })
              // 表字段名与原始字段名对应Map
              leftScopeChild.setRawDataFields(mapAsJavaMap(leftFieldWithTypes.map(f => (f._2, f._1)).toMap))
              // RowTypeInfo
              leftScopeChild.setRowTypeInfo(new RowTypeInfo(leftFieldWithTypes.map(_._3), leftFieldWithTypes.map(_._2)))

              // 左表的流
              ds.map(map2Row(leftFieldWithTypes))(leftScopeChild.getRowTypeInfo)
            case None =>
              leftScopeChild.setRowTypeInfo(new RowTypeInfo(targetTable._1.getSchema.getTypes, targetTable._1.getSchema.getColumnNames))
              // 左表的流
              tableEnv.toAppendStream[Row](targetTable._1)(leftScopeChild.getRowTypeInfo)
          }

          // 设置Join 的 Right table 信息
          val rightScopeChild = new JoinScope.ScopeChild
          // 别名
          rightScopeChild.setAlias(joinInfo.getRightTableAlias)
          // 表名称
          rightScopeChild.setTableName(joinInfo.getRightTableName)
          // the TypeInfo of Join right table
          val sideTable = sideTables.get(joinInfo.getRightTableAlias) match {
            case Some(t) => t
            case None => sideTables(joinInfo.getRightTableName)
          }
          val rightFieldWithTypes = TableUtils.toTypeInfo(sideTable)
          // 表字段名与原始字段名对应Map
          rightScopeChild.setRawDataFields(mapAsJavaMap(rightFieldWithTypes.map(f => (f._2, f._1)).toMap))
          // RowTypeInfo
          rightScopeChild.setRowTypeInfo(new RowTypeInfo(rightFieldWithTypes.map(_._3), rightFieldWithTypes.map(_._2)))


          joinScope.addScope(leftScopeChild)
          joinScope.addScope(rightScopeChild)


          //获取两个表的所有字段
          val sideJoinFieldInfo = getAllField(joinScope)


          // 参与join （左表和右表）的字段信息,eg:(tableName, tableFieldName, outFieldName)
          val mappingTable: HashBasedTable[String, String, String] = HashBasedTable.create[String, String, String]()
          val sideOutTypeInfo = buildOutRowTypeInfo(sideJoinFieldInfo, mappingTable)
          val sideInfo = TableUtils.getSideInfo(joinInfo, leftScopeChild.getRowTypeInfo, sideJoinFieldInfo.toList)

          // 数据源配置列表
          val tableDataSources = tables.map(elem => (elem._1.tableName.toUpperCase, elem._2))

          // Join 后的DataStream
          val dsOut = getDataStream(inputStream, tableDataSources(joinInfo.getRightTableName), sideTable, sideInfo, sideOutTypeInfo)


          // Join 后表名称
          val targetTableName = joinInfo.getNewTableName
          // Join 后表别名
          val targetTableAlias = joinInfo.getNewTableAlias

          val replaceInfo = new FieldReplaceInfo
          replaceInfo.setMappingTable(mappingTable)
          replaceInfo.setTargetTableName(targetTableName)
          replaceInfo.setTargetTableAlias(targetTableAlias)

          replaceInfoList.add(replaceInfo)

          logger.info(s"side table registerDataStream ${joinInfo.getNewTableName}")
          //注册维表Join
          tableEnv.registerDataStream(joinInfo.getNewTableName, dsOut)

      }

      pollObj = exeQueue.poll()
    }

    fSql
  }

  def getDataStream(in: DataStream[Row], ds: DataSource, table: com.haima.sage.bigdata.etl.common.model.Table, info: SideInfo, sideOutTypeInfo: RowTypeInfo): DataStream[Row] = {
    getReqRow(ds, table, info) match {
      case allReqRow: AllReqRow =>
        in.flatMap(allReqRow)(sideOutTypeInfo)
      case asyncReqRow: AsyncReqRow =>
        new DataStream(AsyncDataStream.orderedWait(in.javaStream, asyncReqRow, 180000, TimeUnit.MILLISECONDS, 10).returns(sideOutTypeInfo).setParallelism(1))
      case _ =>
        null
    }
  }

  /**
    * 获取 维表 ReqRow
    *
    * @param ds
    * @param info
    * @return
    */
  def getReqRow(ds: DataSource, table: com.haima.sage.bigdata.etl.common.model.Table, info: SideInfo): ReqRow = {
    val name: String = Constants.CONF.getString(s"app.flink.sql.sink.table.${table.sideTable.name}.${ds.name}")
    val clazz = Class.forName(name).asInstanceOf[Class[ReqRow]]
    clazz.getConstructor(ds.getClass, table.sideTable.getClass, classOf[SideInfo]).newInstance(ds, table.sideTable, info)
  }

  /**
    * DataStream[RichMap] to DataStream[Row]
    *
    * @param fieldWithTypes Array(原始字段名称, 表字段名称, TypeInformation)
    * @return
    */
  private def map2Row(fieldWithTypes: Array[(String, String, TypeInformation[_])]): RichMap => Row = event => {

    val row = new Row(fieldWithTypes.length)
    fieldWithTypes.zipWithIndex.foreach {
      case (tuple, index) =>
        row.setField(index, event.get(tuple._1) match {
          case Some(d: Date) if Types.LONG == tuple._3 =>
            d.getTime
          case Some(d: Date) if Types.SQL_DATE == tuple._3 =>
            new sql.Date(d.getTime)
          case Some(d: Date) if Types.SQL_TIMESTAMP == tuple._3 =>
            new Timestamp(d.getTime)
          case Some(d: Date) if Types.SQL_TIME == tuple._3 =>
            new Time(d.getTime)
          case Some(d) => d
          case None => null
        })
    }
    row

  }
}
