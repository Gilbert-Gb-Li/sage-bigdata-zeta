package com.haima.sage.bigdata.etl.server.knowledge

import java.sql.{Connection, ResultSet}
import java.util

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.driver.JDBCDriver
import com.haima.sage.bigdata.etl.knowledge.KnowledgeLoader
import com.haima.sage.bigdata.etl.utils.Logger

import scala.util.{Failure, Success, Try}

class JDBCKnowledgeLoader(override val source: JDBCSource) extends KnowledgeLoader[JDBCSource, Map[String, Any]] with Logger {
  private lazy val driver = JDBCDriver(source)
  private lazy val table = source.tableOrSql


  lazy val iterator = new Iterator[RichMap] with AutoCloseable {

    import scala.collection.JavaConversions._

    private lazy val connect: Connection = {
      driver.driver() match {
        case Success(connection) =>
          connection
        case Failure(e) =>
          throw e

      }
    }
    private var sql = ""
    if(table.toLowerCase.contains("from")){
      sql = table
    }else{
      sql = s"select * from ${table} as tmp"
    }
    private lazy val statement = connect.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    private lazy val set = statement.executeQuery()
    private lazy val meta = set.getMetaData

    override def hasNext: Boolean = {
      set.next()
    }

    override def next(): RichMap = {
      (1 to meta.getColumnCount)
        .map { i =>
          val data = Try(set.getObject(i))
          if (data.isSuccess) {
            (meta.getTableName(i), meta.getColumnName(i), data.get)
          } else {
            logger.debug("makeData get null, now index is " + i)
            (meta.getTableName(i), meta.getColumnName(i), null)
          }
        }.groupBy(_._1) match {
        case data =>
              data.flatMap(_._2.map(row => {
              row._3 match {
                case d:java.lang.Long=>
                  (row._2, d.longValue())
                case d: java.sql.Timestamp =>
                  (row._2, new java.util.Date(d.getTime))
                case d: java.math.BigDecimal =>
                  (row._2, d.doubleValue())
                case d: java.math.BigInteger =>
                  (row._2, d.longValue())
                case d: scala.math.BigDecimal =>
                  (row._2, d.doubleValue())
                case d: scala.math.BigInt =>
                  (row._2, d.longValue())
                case d =>
                  (row._2, d)
              }
            }
              )).toMap

//              if (data.size == 1) {
//            TODO multi tables
//              } else {
//                val pah = data.map {
//                  case (tableName, d) =>
//                    (tableName, d.map(row => {
//                      row._3 match {
//                        case d: java.sql.Timestamp =>
//                          (row._2, new java.util.Date(d.getTime))
//                        case _d =>
//                          (row._2, _d)
//                      }
//                    }).toMap)
//                }
//
//
//                val pah1 = pah - ""
//                if (pah1.size == 1) {
//                  pah1.head._2.toMap.++(pah.getOrElse("", Map()).asInstanceOf[java.util.Map[String, Any]])
//                } else {
//                  /*
//                  *
//                  * 当是视图时,或者多表联合查询时,""表示不属于任何表的数据;
//                  * */
//                  pah.get("") match {
//                    case Some(d)=>
//                      pah1.++(d)
//                    case _=>
//                      pah1
//                  }
//                }
//              }
                  }
                //}).toMap)
//            }
//            val pah1: Map[String, Map[String, AnyRef]] = pah.-("")
//            if (pah1.size == 1) {
//              pah1.head._2.++(pah.getOrElse("", Map()).asInstanceOf[java.util.Map[String, Any]])
//            } else {
//              /*
//                  *
//                  * 当是视图时,或者多表联合查询时,""表示不属于任何表的数据;
//                  * */
//              pah.get("") match {
//                case Some(d)=>
//                  pah1.++(d)
//                case _=>
//              }
//              var pah2: Map[String, AnyRef] = Map[String, util.Map[String, AnyRef]]()
//              pah1.foreach(p=>{
//                pah2 = pah2++ p._2.toMap
//              })
//              pah2
//            }
//
//          }
     // }
    }

    override def close(): Unit = {
      try {
        set.close()
        statement.close()
        connect.close()
      } catch {
        case e: Exception =>
          logger.error("ignore jdbc close error {}", e.getMessage)
      }
    }
  }

  /** 加载数据接口 **/
  override def load(): Iterable[Map[String, Any]] = iterator.toIterable

  /**
    * 分页数据大小
    *
    * @return (数据,是否还有)
    */
  override def byPage(size: Int): (Iterable[Map[String, Any]], Boolean) = {

    val data = iterator.take(10000).toList
    if (data.size >= 10000) {
      (data, true)
    } else {
      (data, false)
    }
  }
}
