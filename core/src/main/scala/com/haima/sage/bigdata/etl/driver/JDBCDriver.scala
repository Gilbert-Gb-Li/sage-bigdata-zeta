package com.haima.sage.bigdata.etl.driver

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.haima.sage.bigdata.etl.common.model.{JDBCSource, JDBCWriter, WithProperties}
import com.haima.sage.bigdata.etl.utils.Logger

import scala.util.Try

/**
  * Created by zhhuiyan on 2016/11/22.
  */
case class JDBCDriver(mate: JDBCMate) extends Driver[Connection] with WithProperties with Logger {

  private var drive: java.sql.Driver = _

  def driver(): Try[Connection] = {
    val (url, clazz) = mate.protocol match {
      case "sqlite" =>
        mate.driver match {
          case "org.sqlite.JDBC"=>
            (s"jdbc:sqlite:${mate.schema}", s"${mate.driver}")
          case _=>
            (s"jdbc:sqlite:${mate.schema}", s"${mate.driver}")
        }
      case "derby" =>
        mate.driver match {
          case "org.apache.derby.jdbc.EmbeddedDriver"=>
            (s"jdbc:derby:${mate.schema}", s"${mate.driver}")
          case _=>
            (s"jdbc:derby:${mate.schema}", s"${mate.driver}")
        }
      case "mysql" =>
        mate.driver match {
          case "com.mysql.jdbc.Driver"=>
            (s"jdbc:mysql://${mate.host}:${mate.port}/${mate.schema}?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&characterEncoding=${get("encoding", "utf-8")}", s"${mate.driver}")
          case _=>
            (s"jdbc:mysql://${mate.host}:${mate.port}/${mate.schema}?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&characterEncoding=${get("encoding", "utf-8")}", s"${mate.driver}")
        }
      case "oracle" =>
        /*TODO SID or server_Name*/
        mate.driver match {
          case "oracle.jdbc.driver.OracleDriver"=>
            println(s"oracle driver is ${(s"jdbc:oracle:thin:@//${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")}")
            // jdbc:oracle:thin:@//10.27.76.145:1521/GFEDM02
            (s"jdbc:oracle:thin:@//${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
          case _=>
            println((s"jdbc:oracle:thin:@//${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}"))
            // jdbc:oracle:thin:@//10.27.76.145:1521/GFEDM02
            (s"jdbc:oracle:thin:@//${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
        }
      case "db2" =>
        mate.driver match {
          case "com.ibm.db2.jcc.DB2Driver"=>
            (s"jdbc:db2://${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
          case _=>
            (s"jdbc:db2://${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
        }
      case "sybase" =>
        mate.driver match {
          case "net.sourceforge.jtds.jdbc.Driver"=>
            (s"jdbc:jtds:sybase://${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
          case "com.sybase.jdbc3.jdbc.SybDriver"=>
            val  useMateData = mate match {
              case _:JDBCSource | _:JDBCWriter => mate.properties.get.getProperty("useMatedata").toBoolean
            }
            if(!useMateData)
              (s"jdbc:sybase:Tds:${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
            else
              (s"jdbc:sybase:Tds:${mate.host}:${mate.port}?LITERAL_PARAMS=true&LANGUAGE=english&CHARSET=${get("encoding", "utf8")}&APPLICATIONNAME=${mate.schema}", s"${mate.driver}")
          case _=>
            (s"jdbc:jtds:sybase://${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
        }
      case "postgresql" =>
        mate.driver match {
          case "org.postgresql.Driver"=>
            (s"jdbc:postgresql://${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
          case _=>
            (s"jdbc:postgresql://${mate.host}:${mate.port}/${mate.schema}", s"${mate.driver}")
        }
      case "sqlserver" =>
        mate.driver match {
          case "net.sourceforge.jtds.jdbc.Driver"=>
            (s"jdbc:jtds:sqlserver://${mate.host}:${mate.port}/${mate.schema}",  s"${mate.driver}")
          case "com.microsoft.sqlserver.jdbc.SQLServerDriver" =>
            (s"jdbc:sqlserver://${mate.host}:${mate.port};DatabaseName=${mate.schema}", s"${mate.driver}" )
          case _=>
            (s"jdbc:sqlserver://${mate.host}:${mate.port};DatabaseName=${mate.schema}", s"${mate.driver}" )
        }
      case "phoenix" =>
        mate.driver match {
          case "org.apache.phoenix.jdbc.PhoenixDriver"=>
            (s"jdbc:phoenix:${mate.host}:${mate.port}",  s"${mate.driver}")
          case _=>
            (s"jdbc:phoenix:${mate.host}:${mate.port}",  s"${mate.driver}")
        }
      case _ =>
        logger.warn("NotImplementedError ")
        throw new NotImplementedError()
    }
    if (drive != null) {
      DriverManager.deregisterDriver(drive)
    }
   drive = Class.forName(clazz).newInstance().asInstanceOf[java.sql.Driver]
    DriverManager.registerDriver(drive)
    Try(DriverManager.getConnection(url, get("user").getOrElse(""), get("password").getOrElse("")))
  }


  override def properties: Option[Properties] = mate.properties
}
