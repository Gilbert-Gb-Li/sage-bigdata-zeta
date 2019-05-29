package com.haima.sage.bigdata.etl.server.knowledge

import java.io.BufferedReader
import java.net.URLDecoder
import java.sql.{Clob, Connection, PreparedStatement}

import akka.actor.{Actor, ActorIdentity, ActorRef, Identify, Terminated}
import com.haima.sage.bigdata.etl.common.model.{Opt, Status}
import com.haima.sage.bigdata.etl.store.Store
import com.haima.sage.bigdata.etl.utils.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by liyju on 2017/12/18.
  */
case class StoredKnowledgeUserServer(table: String) extends Actor with Logger {
  import context.dispatcher
  private val userList = new mutable.ListBuffer[ActorRef]()

  override def preStart(): Unit = {

  }

  import scala.collection.JavaConversions._

  lazy val stores = Store()

  val conn: Connection = stores.connection

  def validateTableExist(tableName: String): Boolean = {
    try {
      conn.getMetaData.getTables(null, null, tableName, Array("TABLE")).next()
    } catch {
      case _: Exception =>
        false
    }

  }


  private def load(statement: PreparedStatement): Unit = {
    implicit val ord: Ordering[(Map[String, Any], Int, Long)] = Ordering.by {
      data =>
        (data._2 + 1) * data._3
    }

    // 将字Clob转成String类型
    def ClobToString(sc: Clob): String = {
      val sb = new StringBuffer
      Try {
        val is = sc.getCharacterStream
        // 得到流
        val br = new BufferedReader(is)
        var s = br.readLine
        while (s != null) {
          // 执行循环将字符串全部取出付值给StringBuffer由StringBuffer转成STRING
          sb.append(s)
          s = br.readLine
        }
        sb.toString
      } match {
        case Success(_) =>
          sb.toString
        case Failure(e) =>
          e.printStackTrace()
          ""
      }
    }

    val cache: ListBuffer[Map[String, Any]] = new ListBuffer[Map[String, Any]]()
    val rt = statement.executeQuery()

    var i = 1
    while (rt.next()) {
      val meta = rt.getMetaData
      val item = (1 to meta.getColumnCount)
        .map { i =>
          val data = Try(
            meta.getColumnTypeName(i) match {
              case "CLOB" =>
                ClobToString(rt.getClob(i))
              case "BLOB" =>
                new String(rt.getBlob(i).getBytes(1.toLong, rt.getBlob(i).length.asInstanceOf[Int]))
              case _ =>
                rt.getObject(i)
            })
          if (data.isSuccess) {
            (meta.getTableName(i), meta.getColumnName(i).toLowerCase, data.get)
          } else {
            logger.debug("makeData get null, now index is " + i)
            (meta.getTableName(i), meta.getColumnName(i).toLowerCase, null)
          }
        }.groupBy(_._1) match {
        case data =>
          if (data.size == 1) {
            data.head._2.map(row => {
              row._3 match {
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
                case d if d != null =>
                  (row._2, d.toString)
                case d =>
                  (row._2, d)
              }
            }
            ).toMap
          } else {
            val pah = data.map {
              case (tableName, d) =>
                (tableName, d.map(row => {
                  row._3 match {
                    case d: java.sql.Timestamp =>
                      (row._2, new java.util.Date(d.getTime))
                    case _d =>
                      (row._2, _d)
                  }
                }).toMap)
            }
            val pah1 = pah - ""
            if (pah1.size == 1) {
              pah1.head._2.toMap.++(pah.getOrElse("", Map()).asInstanceOf[java.util.Map[String, Any]])
            } else {
              pah1.++(pah.getOrElse("", Map()).asInstanceOf[java.util.Map[String, Any]])
            }
          }
      }

      cache.add(item)

      if (cache.size == size) {

        logger.debug(s"sync data ${size * (i + 1)}")
        sender() ! (cache.toList, false)
        i += 1
        cache.clear()
      }
    }
    if (cache.nonEmpty) {
      logger.debug(s"sync data last ${cache.size}")
      sender() ! (cache.toList, true)
    }else{
      sender() ! (List[Map[String, Any]](), true)
    }

    if (rt != null && !rt.isClosed)
      rt.close()

  }


  val size = 1000
  //var current = 0

  /** 加载数据接口 **/
  def load(current: Int): Unit = {
    logger.debug(s"==========select * from $table OFFSET ${current * size} ROWS FETCH NEXT ${size} ROWS ONLY========current:${current}")
    //如果存在表
    if (validateTableExist(table)) {
      val sql = "select * from \"" + table + "\""
      val statement = conn.prepareStatement(sql)
      load(statement)
      if (statement != null && !statement.isClosed)
        statement.close()
    }
  }


  override def receive: Receive = {


    //知识库同步
    case (Opt.SYNC, current: Int) =>
      val start = System.currentTimeMillis()
      logger.debug(s"sync table[${table}] $current start")
      load(current)

      logger.debug(s"sync data $current take data ${System.currentTimeMillis() - start} ms")


    //知识库同步完成
    case (Opt.SYNC, Status.FINISHED) =>
      logger.debug(s"KNOWLEDGE[$table] SYNC FINISHED!")
      val path = URLDecoder.decode(sender().path.toString.replaceAll("akka://master/system/receptionist/", ""),
        "UTF8")
      val user = context.actorSelection(path)
      user ! Identify(path)
      sender() ! (Opt.LOAD, Status.FINISHED) //知识库同步完成

    case Terminated(actor) =>
      context.unwatch(actor)
      context.system.scheduler.scheduleOnce(3 seconds) {
        val path = URLDecoder.decode(sender().path.toString.replaceAll("akka://master/system/receptionist/", ""),
          "UTF8")
        identifying(path)
      }

    //重新请求到了knowledge user 提供的actorRef，请求更新知识库
    case ActorIdentity(_, Some(ref)) =>
      if (!userList.contains(ref)) {
        userList.append(ref)
        context.watch(ref)
      }

    //没有请求到knowledge user 提供的actorRef
    case ActorIdentity(_, None) =>
      logger.warn(s"unknown identity[KNOWLEDGE]")

    //通知知识库的使用者更新知识库缓存
    case ("KNOWLEDGE", "UPDATE") =>
      logger.debug(s"KNOWLEDGE UPDATE START!")
      userList.foreach(user => {
        user ! ("KNOWLEDGE", "UPDATE", "RELOAD")
      }
      )

    case msg =>
      logger.warn(s"unknown msg:${msg}")
  }

  // send identify info to acquire an ActorRef
  private def identifying(path: String) = {
    logger.debug(s" identifying  helper[$path].")
    context.actorSelection(path) ! Identify(path)
  }
}
