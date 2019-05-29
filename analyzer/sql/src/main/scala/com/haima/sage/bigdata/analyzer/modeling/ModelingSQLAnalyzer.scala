package com.haima.sage.bigdata.analyzer.modeling

import java.util.{Calendar, Date}

import com.haima.sage.bigdata.analyzer.SqlDataAnalyzer
import com.haima.sage.bigdata.analyzer.sql.udf.{Date2Long, Long2Date, PercentRank}
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, RichMap, SQLAnalyzer, Table}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, _}
import org.apache.flink.types.Row

/**
  * Created by evan on 17-8-29.
  */
class ModelingSQLAnalyzer(override val conf: SQLAnalyzer,
                          override val `type`: AnalyzerType.Type = AnalyzerType.MODEL)
  extends DataModelingAnalyzer[SQLAnalyzer](conf)
    with SqlDataAnalyzer[BatchTableEnvironment, DataSet[RichMap]] {


  override lazy val isModel: Boolean = false

  def getEnvironment(data: DataSet[RichMap]): BatchTableEnvironment = {
    val env = data.getExecutionEnvironment
    env.setParallelism(CONF.getInt("flink.parallelism"))
    TableEnvironment.getTableEnvironment(env)
  }

  def register(data: DataSet[RichMap], first: Table)(implicit environment: BatchTableEnvironment): org.apache.flink.table.api.Table = {
    val inFirst = typeTags(first)
    val table = data.filter(_.nonEmpty).map(map2Row(inFirst._1))(inFirst._2, inFirst._3).toTable(environment) // tEnv.fromDataStream(rows)
    environment.registerTable(first.tableName, table)
    table
  }

  import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}


  override def analyze(environment: BatchTableEnvironment, sql: Option[String], fieldsDic: Option[Map[String, String]] = None): DataSet[RichMap] = {

    environment.registerFunction("PercentRank", new PercentRank())
    environment.registerFunction("Long2Date", new Long2Date())
    environment.registerFunction("Date2Long", new Date2Long())
    val queueSql = sql match {
      case Some(s) => s
      case None => conf.sql
    }
    val rTb = environment.sqlQuery(transform(queueSql, environment))
    val columnNames = rTb.getSchema.getColumnNames
    rTb.toDataSet[Row].map(tuple => {
      RichMap(columnNames.zipWithIndex.map {
        case (key, index) => {
          fieldsDic match {
            case Some(dic) => dic.getOrElse(key, key)
            case None => key
          }
        } -> (tuple.getField(index) match {

          /*补充数据到东八区*/
          case d: Date =>
            val instance = Calendar.getInstance()
            instance.setTime(d)
            instance.getTime
          case v =>
            v
        })
      }.toMap)
    })
  }

  override def action(data: DataSet[RichMap]): DataSet[RichMap] = {
    conf.table match {
      case Some(t) =>
        val env = getEnvironment(data)
        register(data, t)(env)
        analyze(env)
      case _ =>
        throw new NotImplementedError("redo analyzer for sql not table set is not suppert")
    }

  }

  /**
    * 加载模型
    *
    * @return
    */
  override def load()(implicit env: ExecutionEnvironment): Option[DataSet[RichMap]] = ???

  /**
    * 训练模型
    *
    * @param data
    * @return
    */
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = ???

  /**
    * 执行分析
    *
    * @param data
    * @param model
    * @return
    */
  override def actionWithModel(data: DataSet[RichMap], model: Option[DataSet[RichMap]]): DataSet[RichMap] = ???

  /**
    * 算法是否可以进行“生产模型”
    *
    * @return
    */
  override def modelAble: Boolean = false
}
