package com.haima.sage.bigdata.etl.reader

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.junit.Test

/**
  * Created by zhhuiyan on 16/1/15.
  */
class JDBCReaderTest {
  @Test
  def readMysql(): Unit = {
    val (url, driver) = (s"jdbc:mysql://127.0.0.1:3306/test", "com.mysql.jdbc.Driver")
    val properties: Properties = new Properties
    val MAX = s"SELECT * FROM P_BUSI_QUERY"
    properties.put("user", "root")
    properties.put("password", "root")
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, properties)
    val set = connection.prepareStatement(MAX).executeQuery()
    import scala.collection.JavaConversions._
    if (set.next()) {
      val meta = set.getMetaData
      val item = (1 to meta.getColumnCount)
        .map { i =>
          (meta.getTableName(i), meta.getColumnName(i),
            set.getObject(i))
        }.groupBy(_._1) match {
        case data =>
          if (data.size == 1) {
            data.head._2.map(row => (row._2, row._3)).toMap
          } else {
            val pah = data.map {
              case (tableName, d) =>
                (tableName, d.map(row => (row._2, row._3)).toMap)
            }

            (pah - "").++(pah.getOrElse("", Map()).asInstanceOf[java.util.Map[String, Any]])
          }
      }

      item.get("PROC_INS_END_TIME").foreach(LIMI_MONEY => {
        if (LIMI_MONEY.isInstanceOf[java.math.BigDecimal]) {
          println(LIMI_MONEY.asInstanceOf[java.math.BigDecimal].doubleValue())
        }
        println(LIMI_MONEY)

        val d = new java.math.BigDecimal("2000000000000.000001")
        println(d, d.getClass)
        println(d.doubleValue())
      }

      )
      println()
    }

    /*


     val value= if (set.next()) {
       set.getObject(1)
     } else {
       0
     }
   println(value.getClass)*/

  }
}
