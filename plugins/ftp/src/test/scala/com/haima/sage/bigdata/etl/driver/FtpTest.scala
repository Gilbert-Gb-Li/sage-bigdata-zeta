package com.haima.sage.bigdata.etl.driver

import com.haima.sage.bigdata.etl.common.exception.LogProcessorException
import com.haima.sage.bigdata.etl.common.model.ReadPosition
import com.haima.sage.bigdata.etl.monitor.file.FTPFileWrapper
import com.haima.sage.bigdata.etl.reader.TXTFileLogReader
import com.haima.sage.bigdata.etl.utils.PathWildMatch
import org.apache.commons.net.ftp.{FTPClient, FTPReply}
import org.junit.Test

/**
  * Created by zhhuiyan on 2017/4/19.
  */
class FtpTest {
  def fs(): FTPClient = {
    val client = new FTPClient()
    client.connect("172.16.219.173", 21)
    client.enterLocalPassiveMode()
    client.login("ftpuser", "123456")
    val reply = client.getReplyCode
    if (!FTPReply.isPositiveCompletion(reply)) {
      client.disconnect()
      throw new LogProcessorException("FTP server refused connection. ")
    }
    client
  }

  @Test
  def ls(): Unit = {
    val client = new FTPClient()
    client.connect("10.10.100.58", 21)
    client.enterRemotePassiveMode()
    //client.enterLocalPassiveMode()
    client.login("bigdata", "bigdata")
    val reply = client.getReplyCode
    if (!FTPReply.isPositiveCompletion(reply)) {
      client.disconnect()
      throw new LogProcessorException("FTP server refused connection. ")
    }
    client.listFiles("/home/bigdata/ftp/backup").foreach(println)
  }
}
