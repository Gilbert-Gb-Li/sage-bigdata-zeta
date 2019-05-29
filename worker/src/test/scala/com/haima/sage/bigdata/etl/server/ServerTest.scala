package com.haima.sage.bigdata.etl.server

import java.sql.{Connection, DriverManager, ResultSet}

import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.model.{Config, Status}
import com.haima.sage.bigdata.etl.store.{ConfigStore, Stores}
import com.haima.sage.bigdata.etl.utils.Mapper
import org.junit.Test

import scala.collection.mutable

/**
  * Created by zhhuiyan on 2015/3/6.
  */
class ServerTest extends Mapper {


  @Test def test1() = {
    val status = Status.PENDING
    println(status.toString)
  }

  @Test
  def classes(): Unit = {
    val files = "codec" :: "reader" :: "monitor" :: "lexer" :: "writer" :: "stream.handler" :: Nil
    println(files.map {
      name =>
        (name, CONF.getConfig("app." + name).root().unwrapped())
    }.toMap)
  }

  @Test def test2() = {
    Class.forName("org.sqlite.JDBC")
    val conn: Connection = DriverManager.getConnection("jdbc:sqlite:./db")
    val sql = "SELECT STATUS_TYPE_VALUE FROM STATUS_INFO WHERE DS_ID = ? AND STATUS_TYPE = ?;"
    val stmt = conn.prepareStatement(sql)
    stmt.setString(1, "0")
    stmt.setString(2, "Status")
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next()) {
      val value = rs.getString("STATUS_TYPE_VALUE")
      if (value != null && "".equals(value))
        printf("111")
    }
  }

  @Test def test3() = {

    val id: String = "1"
    id match {
      case null =>
        println("true")
      case id: String =>
        println(id)
    }
  }

  @Test def test4() = {
    val strings: Array[String] = new Array[String](1)
    strings(0) = "TABLE"
    println(strings(1))
  }

  @Test def test5() = {
    val SELECT_TABLE_FOR_NAME: String = "CONFIG_INFO,DATA_SOURCE_INFO,METADATA_INFO,COLLECTOR_INFO,WRITER_INFO,STATUS_INFO"
    for (str <- SELECT_TABLE_FOR_NAME.split(",")) {
      println(str)
    }
  }

  @Test def test6(): Unit = {
    val configStore: ConfigStore = Stores.configStore

    val config = mapper.readValue[Config]("{\"collector\":{\"name\":\"pipe\",\"system\":\"agent\",\"host\":\"127.0.0.1\",\"cache\":0,\"id\":\"73618a27-4402-416e-9417-2ba2a0b6a0cb\",\"port\":5151},\"parser\":{\"filter\":[{\"fields\":{\"test\":\"124\"},\"name\":\"addField\"}],\"name\":\"cef\"},\"writers\":[{\"name\":\"es\",\"hostPorts\":[[\"172.16.219.171\",9200]],\"esType\":\"syslog\",\"cache\":1,\"cluster\":\"elasticsearch12\",\"id\":\"3\",\"index\":\"logs_%{yyyyMMdd}\"}],\"dataSource\":{\"name\":\"net\",\"host\":\"172.16.219.179\",\"cache\":1,\"port\":4301,\"contentType\":\"syslog\",\"properties\":{},\"metadata\":{\"host\":\"localhost\",\"data\":[[\"test\",\"test\",\"String\"]],\"id\":\"3\",\"port\":1,\"app\":\"app1\"},\"protocol\":\"udp\"},\"id\":\"3\"}");
   // configStore.insert(config)
  }

  @Test def test7(): Unit = {
    /*  val derby = new DerbyConfigStore()
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
      val url = "jdbc:derby:C:\\Users\\Xr\\Desktop\\derby"
      derby.conn = DriverManager.getConnection(url, null, null)
      derby.conn.setAutoCommit(true)
      //derby.deleteConfigStatus("123");
      val status = derby.status("")
      status.foreach(
        configs =>
          configs._2.foreach(
              configList =>
                if("Status".equals(configList._2)){
                  if("STOPPED".equals(configList._3)){
                    status.remove(configs._1)
                  }
                }
          )
      )
      println(status)*/
  }

  @Test def test8() = {
    var map = mutable.HashMap(1 -> "a", 2 -> "b", 3 -> "c")
    map += (4 -> "d")
    map.foreach(value => print(value + " "))
  }
}
