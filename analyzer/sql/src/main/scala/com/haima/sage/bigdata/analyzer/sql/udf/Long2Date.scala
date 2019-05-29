package com.haima.sage.bigdata.analyzer.sql.udf

import java.sql.Date

import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.ScalarFunction

/**
  * Created by zhonghuiyan on 17-8-9.
  */
class Long2Date extends ScalarFunction with Logger {


  /*
  * SQL 中 unix timestamp to sql Date
  *
  *  例如  Long2Date(131221313123131);
  *
  * */
  def eval(data: Long): Date = {
    new Date(data)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = Types.SQL_DATE
}
