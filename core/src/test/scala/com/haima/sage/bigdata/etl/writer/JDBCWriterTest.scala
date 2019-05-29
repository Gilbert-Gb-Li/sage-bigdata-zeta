package com.haima.sage.bigdata.etl.writer

import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random}

import com.haima.sage.bigdata.etl.utils.Logger
import org.junit.Test

/**
  * Created by zhhuiyan on 2017/2/15.
  */
class JDBCWriterTest extends Logger {

  @Test
  def db2: Unit = {
    val (url, driver) = (s"jdbc:db2://127.0.0.1:50000/SAMPLE", "com.ibm.db2.jcc.DB2Driver")
    val properties: Properties = new Properties

    properties.put("user", "db2inst1")
    properties.put("password", "db2inst1")
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, properties)

    new Timestamp(new Date().getTime) match {
      case d: Date =>
        println(d)
    }
    (1 to 1000).foreach(i => {
      val sql = s"INSERT INTO DB2INST1.WEALTHCUSTOMER (CUSTOMERID, CUSTNO, REGION, NAME, GENDER, GROUPID, ISBIND, CUSTTYPE, BIRTHDAY, TELNUM) VALUES ('$i', '${i + 117191278}', '110', '这个人-$i', '1', '${i / 100}', '1', '  ', '20140203', '13145732234')"
      val statement = connection.prepareStatement(sql)
      statement.execute()
      statement.close()
    }
    )

    val sqll =
      """SELECT *
        |FROM (SELECT
        |        SUBSTR(GROUPID, 1, 5) AS GROUPID,
        |        ROWID,
        |        CUSTOMERID,
        |        CUSTNO,
        |        REGION,
        |        NAME,
        |        GENDER,
        |        NOTEID,
        |        ISBIND,
        |        CUSTTYPE,
        |        BIRTHDAY,
        |        TELNUM,
        |        MOSTLEVEL,
        |        CAREFULID,
        |        UPDATETIME
        |      FROM WEALTHCUSTOMER WITH UR) AS tmp
        |WHERE tmp.ROWID > 0 AND tmp.ROWID <= 36000
        |ORDER BY tmp.ROWID ASC""".stripMargin

    val statement = connection.prepareStatement(sqll, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val rt = statement.executeQuery()
    if (rt.next()) {
      println(rt.getInt(2))
    }
    rt.close()
    connection.close()
  }

  @Test
  def mysql: Unit = {
    //    val (url, driver) = (s"jdbc:db2://127.0.0.1:50000/SAMPLE", "com.ibm.db2.jcc.DB2Driver")
    val random: Random = new Random
    val dateSimpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dateTimeSimpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timeSimpleDateFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    val (url, driver) = ("jdbc:mysql://localhost:3306/test", "com.mysql.jdbc.Driver")
    val properties: Properties = new Properties

    properties.put("user", "root")
    properties.put("password", "root")
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, properties)

    new Timestamp(new Date().getTime) match {
      case d: Date =>
        println(d)
    }
    (1 to 10000000).foreach(i => {
      val sql = s"INSERT INTO test.om_auaudpicp(AudAudSts,AudEntUsr,AudEntDat,AudBrnNbr,AudBbkNbr,AudEntNbr,AudPicID,AudTskNbr,AudEvtNbr,AudPrdNbr,AudCcyNbr,AudTrxAmt,AudLgrDat,AudEacNbr,AudOwnCIt,AudCItTyp,AudPsbCod,AudBilNbr,AudTskEnd,AudMntUsr,AudMntDat,AudMntTim,AudTskCod,AudWkeCod,AudRtnCod,AudRecVer,AudPicFlg,AudSpc079,CreatorId,CreateTime,LastModifyId,LastModifyTime) VALUES ('S','000000', '$getDate', '000000', '000', '1510000', '99901703313300000003A0', '${1000000000000l + i}', '00000000000', '0000000000','00',1.00,'$getDate','000000','0000000000','客户类型','0000','','000','000000','$getDate','$getTime','00000000','00000000','0000000',0,'Y','','SYS001','$getDateTime','SYS001','$getDateTime')"
      //      logger.debug(sql)
      val statement = connection.prepareStatement(sql)
      statement.execute()
      statement.close()
      if (i % 10000 == 0) {
        logger.debug(i+"====")
      }
    }
    )

    def getDate: String = {
      val difference: Int = random.nextInt(1000 * 60 * 60 * 24 * 30 * 12)
      val time = System.currentTimeMillis() - difference
      dateSimpleDateFormat.format(time)
    }

    def getTime: String = {
      val difference: Int = random.nextInt(1000 * 60 * 60 * 24 * 30 * 12)
      val time = System.currentTimeMillis() - difference
      timeSimpleDateFormat.format(time)
    }

    def getDateTime: String = {
      val difference: Int = random.nextInt(1000 * 60 * 60 * 24 * 30 * 12)
      val time = System.currentTimeMillis() - difference
      dateTimeSimpleDateFormat.format(time)
    }

    logger.debug("finish.")
    connection.close()
  }
}
