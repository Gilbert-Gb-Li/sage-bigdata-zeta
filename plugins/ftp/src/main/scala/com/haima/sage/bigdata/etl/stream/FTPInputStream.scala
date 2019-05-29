package com.haima.sage.bigdata.etl.stream

import java.io.InputStream

import org.apache.commons.net.ftp.FTPClient

/**
 * Created by zhhuiyan on 15/6/3.
 */
case class FTPInputStream( fs:FTPClient, stream: InputStream ) extends InputStream{
  override def read(): Int = {
    try{
      stream.read()
    }catch {
      case e:Exception=>
        e.printStackTrace()
        throw e;
    }

  }

  override def close(): Unit = {
    stream.close()
    fs.disconnect()
  }
}
