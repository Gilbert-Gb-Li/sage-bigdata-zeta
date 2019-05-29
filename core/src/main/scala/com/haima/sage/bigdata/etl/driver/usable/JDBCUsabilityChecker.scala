package com.haima.sage.bigdata.etl.driver.usable

import com.haima.sage.bigdata.etl.common.model.{JDBCSource, JDBCWriter, Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.{JDBCDriver, JDBCMate}

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class JDBCUsabilityChecker(mate: JDBCMate) extends UsabilityChecker {

  private lazy val logger = LoggerFactory.getLogger(JDBCUsabilityChecker.getClass)

  val driver = JDBCDriver(mate)
  val msg: String = mate.uri + " has error:"

  def isValid(column:String):Boolean = {
    val regEx = "[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]".r
    val regQuote = "^`\\S*`$".r
    println(regEx.findAllIn(column))
    println(regQuote.findAllIn(column))
    if(!regEx.findAllIn(column).isEmpty && regQuote.findAllIn(column).isEmpty ) {
      return false;
    }else{
      return true
    }
  }

  override def check: Usability = driver.driver() match {
    case Success(connection) =>
      mate match {
        case source: JDBCSource =>
          //判断索引列是否合法
          if(!isValid(source.column)){
            Usability(usable = false, cause = msg + s"Index column contains special symbol, please add back quote.")
            throw new IllegalArgumentException(s"Index column contains special symbol, please add back quote.")
          }else{
            Usability()
          }
          val statement = connection.prepareStatement(source.SELECT_FOR_META)
          val set = statement.executeQuery()
          val meta = set.getMetaData
          val fields = (1 to meta.getColumnCount).map(meta.getColumnName)
          val rt = if (fields.contains(source.column) || fields.contains(source.column.replaceAll("\"","")) || fields.contains(source.column.replaceAll("`",""))) {
            Usability()
          } else {
            Usability(usable = false, cause = msg + s"column[${source.column}] not exist in table[${source.table}]".stripMargin)
          }
          Try {
            set.close()
            statement.close()
            connection.close()
          }
          rt
        case writer: JDBCWriter =>
          val SELECT_FOR_CHECK: String = s"SELECT * FROM ${writer.schema}.${writer.table} where 1=2"
          //验证数据表是否存在
          val usable = Try {
            val statement = connection.prepareStatement(SELECT_FOR_CHECK)
            statement.executeQuery()
          } match {
            case Success(_) =>
              Usability()
            case Failure(_) =>
              Try {
                connection.close()
              }
              Usability(usable = false)
          }
          val usability = writer.protocol match {
            case "phoenix" =>
              //hbase动态表不支持验证,也不支持自动创建表
              if (writer.table.trim.contains("$") || writer.table.trim.replace("？", "?").contains("?")) {
                connection.close()
                Usability(usable = false, cause = msg + s"Dynamic table names are not supported for hbase writer! ")
              }
              else {
                //验证数据表是否存在
                if(usable.usable)
                  Usability()
                else
                  Usability(usable = false, cause = msg + s"org.apache.phoenix.schema.TableNotFoundException: ERROR 1012 (42M03): Table undefined. tableName=${writer.table}")
              }
            case _ =>
              if(usable.usable)
                 Usability()
              else
                 //如果表不存需要校验是否配置了metadata
                if(writer.metadata.isEmpty ||writer.metadata.get.isEmpty )
                  Usability(usable = false, cause = msg + s"TableSchemaNotFoundException: ERROR 1012 (42M03): Table schema undefined. tableName=${writer.table}")
                else
                  //如果设置某个字段是主键，其是否为空字段不能为是。
                writer.metadata match {
                  case Some(fields)=>
                    val rt = fields.filter(column=> {
                      column.pk.contains("Y")&& !column.nullable.contains("N")
                    })
                    if(rt.nonEmpty)
                      Usability(usable = false, cause = msg + s"The column that is set to the primary key cannot be empty。")
                    else{
                      //判断列的经度和长度都不能是小于零的值
                      val rt = fields.filter(column=> {
                        val length:Integer = if(column.length.nonEmpty) {
                          column.length.get
                        }else{
                          0
                        }
                        val decimal :Integer= if(column.decimal.nonEmpty) {
                          column.decimal.get
                        }else{
                          0
                        }
                        length<0 || decimal<0
                      })
                      if(rt.nonEmpty)
                        Usability(usable = false, cause = msg + s"The length and decimal of the column type should not be less than 0 ")
                      else
                        Usability()
                    }

                  case None=>
                    Usability(usable = false, cause = msg + s"TableSchemaNotFoundException: ERROR 1012 (42M03): Table schema undefined. tableName=${writer.table}")
                }
          }
          Try(connection.close())
          usability
      }
    case Failure(e) =>
      logger.error(s"get connect error :"+e)
      Usability(usable = false, cause = msg + s"can't connect to ${mate.protocol} server on '${mate.host}:${mate.port} with ${e.getMessage}'.".stripMargin)

  }
}
