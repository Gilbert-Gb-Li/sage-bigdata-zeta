package com.haima.sage.bigdata.analyzer.streaming

import java.util.{Calendar, Date}

import com.haima.sage.bigdata.analyzer.SqlDataAnalyzer
import com.haima.sage.bigdata.analyzer.sql.udf.{Date2Long, Long2Date, PercentRank}
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{ProctimeAttribute, RowtimeAttribute, UnresolvedFieldReference}
import org.apache.flink.types.Row


/**
  * Created by zhhuiyan on 15/4/23.
  */
class StreamSQLAnalyzer(override val conf: SQLAnalyzer) extends SimpleDataStreamAnalyzer[SQLAnalyzer, Any]
  with SqlDataAnalyzer[StreamTableEnvironment, DataStream[RichMap]] {


  override def getEnvironment(data: DataStream[RichMap]): StreamTableEnvironment = {
    val env = data.executionEnvironment
    env.setParallelism(CONF.getInt("flink.parallelism"))
    TableEnvironment.getTableEnvironment(env)


  }

  override def analyze(environment: StreamTableEnvironment, sql: Option[String], fieldsDic: Option[Map[String, String]] = None): DataStream[RichMap] = {
    environment.registerFunction("PercentRank", new PercentRank())
    environment.registerFunction("Long2Date", new Long2Date())
    environment.registerFunction("Date2Long", new Date2Long())

    val queueSql = sql match {
      case Some(s) => s
      case None => conf.sql
    }
    val rTb = environment.sqlQuery(transform(queueSql, environment))
    val columnNames = rTb.getSchema.getColumnNames
    rTb.toRetractStream[Row].map(tuple => {
      RichMap(
        columnNames.zipWithIndex.map {
          case (key, index) => {
            fieldsDic match {
              case Some(dic) => dic.getOrElse(key, key)
              case None => key
            }
          } -> (tuple._2.getField(index) match {

            /*补充数据到东八区*/
            case d: Date =>
              val instance = Calendar.getInstance()
              instance.setTime(d)
              instance.add(Calendar.MILLISECOND, instance.get(Calendar.ZONE_OFFSET))
              instance.getTime
            case v =>
              v
          })


        }.toMap)
    }
    )
  }

  override def action(data: DataStream[RichMap]): DataStream[RichMap] = {
    conf.table match {
      case Some(t) =>
        val env = getEnvironment(data)
        register(data, t)(env)
        analyze(env)
      case _ =>
        throw new NotImplementedError("re analyzer for sql not table set is not support")
    }

  }

  override def register(data: DataStream[RichMap], first: Table)(implicit environment: StreamTableEnvironment): org.apache.flink.table.api.Table = {
    val flinkTable = table(first, data, environment)
    environment.registerTable(first.tableName, flinkTable)
    flinkTable
  }


  /**
    * build Flink SQL Table
    *
    * @param table : com.haima.sage.bigdata.etl.common.model.Table table info
    * @param data  : DataStream
    * @param tEnv  : StreamTableEnvironment  table Environment
    * @return org.apache.flink.table.api.Table
    */
  def table(table: Table, data: DataStream[RichMap], tEnv: StreamTableEnvironment): org.apache.flink.table.api.Table = {
    val types = typeTags(table, useUpper = true)
    data.setParallelism(CONF.getInt("flink.parallelism"))
    import com.haima.sage.bigdata.analyzer.streaming.utils._
    data.assign(table.timeCharacteristic)

    table.timeCharacteristic match {
      case ProcessingTime() =>
        data.executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.valueOf("ProcessingTime"))
        data.map(map2Row(types._1))(types._2).toTable(tEnv, types._1.map(field => UnresolvedFieldReference(field._1)).toSeq :+ ProctimeAttribute(UnresolvedFieldReference("proctime")): _*) // tEnv.fromDataStream(rows)
      case IngestionTime() =>
        data.executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.valueOf("IngestionTime"))
        data.map(map2Row(types._1))(types._2).toTable(tEnv, types._1.map(field => UnresolvedFieldReference(field._1)).toSeq :+ ProctimeAttribute(UnresolvedFieldReference("proctime")): _*) // tEnv.fromDataStream(rows)
      case EventTime(_field, maxOutOfOrderness) =>
        data.executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.valueOf("EventTime"))
        data.assignTimestampsAndWatermarks(Assigner(_field, maxOutOfOrderness)).map(map2Row(types._1))(types._2).toTable(tEnv, types._1.map(field => {
          Some(field._1) match {
            case Some(`_field`) =>
              RowtimeAttribute(UnresolvedFieldReference(field._1))
            case _ => UnresolvedFieldReference(field._1)
          }
        }).toSeq: _*)
    }
  }


}
