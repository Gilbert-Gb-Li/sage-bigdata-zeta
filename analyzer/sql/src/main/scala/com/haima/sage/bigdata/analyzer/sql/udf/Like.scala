package com.haima.sage.bigdata.analyzer.sql.udf

import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.ScalarFunction

/**
  * Created by chengji on 17-8-9.
  */
class Like extends ScalarFunction with Logger {


  /*
  * SQL 中 直接用字段名表示的是取取值,否则是填写的值
  *
  *  例如  like(a,'%中国%');
  *
  * */
  def eval(s: String, str: String): Boolean = {
    if (str.startsWith("%") && str.endsWith("%"))
      s.contains(str.substring(1, str.length - 1))
    else if (str.startsWith("%"))
      s.endsWith(str.substring(1))
    else if (str.endsWith("%"))
      s.startsWith(str.substring(0, str.length - 1))
    else
      s.equals(str)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = Types.BOOLEAN
}
