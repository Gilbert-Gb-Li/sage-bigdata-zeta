package com.haima.sage.bigdata.analyzer

import java.sql.{Time, Timestamp}
import java.util.Date

import com.haima.sage.bigdata.analyzer.sql.utils.{Lang, TableUtils}
import com.haima.sage.bigdata.etl.common.model.{RichMap, SQLAnalyzer, Table, TableDataAnalyzer}
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.types.Row

import scala.reflect.ClassTag

trait SqlDataAnalyzer[E, T] extends TableDataAnalyzer[E, T, org.apache.flink.table.api.Table] with Serializable with Logger {

  val conf: SQLAnalyzer

  /**
    *
    * @param table    表信息
    * @param useUpper 表字段是否使用大写
    * @return
    */
  private[analyzer] def typeTags(table: Table, useUpper: Boolean = false): (Array[(String, String, TypeInformation[_])], RowTypeInfo, ClassTag[Row]) = {
    logger.debug(s"first:${table.tableName}-${table.fields},sql:${conf.sql}")
    val fieldWithTypes = TableUtils.toTypeInfo(table)
    (fieldWithTypes, new RowTypeInfo(fieldWithTypes.map(_._3), fieldWithTypes.map(f => if (useUpper) f._2 else f._1)), ClassTag(classOf[Row]))
  }


  /**
    * 转换sql
    *
    * @param sql
    * @param tEnv
    * @return
    */
  private[analyzer] def transform(sql: String, tEnv: TableEnvironment): String = {

    Lang.unicode(sql)

    //    // tEnv.registerFunction("zlike", new Like())
    //    logger.info("transforming sql...")
    //    if (isContainChinese(sql) && sql.toLowerCase.contains(" like ")) {
    //
    //
    //      //      //如果包含like语句
    //      //      val p = Pattern.compile("(\\w)\\s+(like|LIKE)\\s+'(%?(\\w|[\\u4e00-\\u9fa5])*%?)'")
    //      //      val m = p.matcher(sql)
    //
    //
    //      //      var newSql = sql
    //
    //      var rt = pattern.replaceAllIn(sql, m => cnToUnicode(m.group(0))) /*replace.replaceAllIn(sql, mather => {
    //        val funName = s"zlike" //获取udf名称
    //
    //
    //        logger.debug(mather.group(3))
    //
    //
    //        s"$funName(${mather.group(1)},'${mather.group(3)}${cnToUnicode(mather.group(4))}${mather.group(5)}')" //拼接子串
    //
    //      })*/
    //
    //      println(rt)
    //      rt
    //      //      while (m.find()) {
    //      //        val funName = s"zlike$i" //获取udf名称
    //      //        tEnv.registerFunction(funName, new Like(m.group(3)))
    //      //        //注册udf
    //      //        val subSql = s"$funName(${m.group(1)})" //拼接子串
    //      //        newSql = newSql.replace(m.group(0), subSql) //替换子串
    //      //        i += 1
    //      //      }
    //      //      logger.info(s"new sql: $newSql")
    //      //      newSql
    //    } else {
    //      logger.info("no need transform sql")
    //      sql
    //    }
  }

  /**
    * 将map数据转换成row数据
    *
    * @return
    */
  private[analyzer] def map2Row(fieldWithTypes: Array[(String, String, TypeInformation[_])]): RichMap => Row = event => {

    val row = new Row(fieldWithTypes.length)
    fieldWithTypes.zipWithIndex.foreach {
      case (tuple, index) =>
        row.setField(index, event.get(tuple._1) match {
          case Some(d: Date) if Types.LONG == tuple._3 =>
            d.getTime
          case Some(d: Date) if Types.SQL_DATE == tuple._3 =>
            new java.sql.Date(d.getTime)
          case Some(d: Date) if Types.SQL_TIMESTAMP == tuple._3 =>
            new Timestamp(d.getTime)
          case Some(d: Date) if Types.SQL_TIME == tuple._3 =>
            new Time(d.getTime)
          case Some(None) => null
          case Some(d) => d
          case None => null
        })
    }
    row

  }

  final def convert(model: Iterable[Map[String, Any]]): Any = {
    throw new NotImplementedError("sql unsupport")
  }

  final def analyzing(in: RichMap, resultFuture: Any): RichMap = {
    throw new NotImplementedError("sql unsupport")
  }

}
