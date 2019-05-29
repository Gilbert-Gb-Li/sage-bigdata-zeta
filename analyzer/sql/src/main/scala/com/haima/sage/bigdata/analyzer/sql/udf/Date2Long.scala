package com.haima.sage.bigdata.analyzer.sql.udf

import java.sql.Timestamp

import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.ScalarFunction

/**
  * Created by liyong on 17-8-9.
  */
class Date2Long extends ScalarFunction with Logger {


  /*
  * SQL 中 Timestapmp 转化成java.lang.long
  *
  *  例如
  *
  * */
  def eval(date: Timestamp): java.lang.Long = {
    if (date == null) {
      0l
    } else {
      date.getTime
    }
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = Types.LONG
}
