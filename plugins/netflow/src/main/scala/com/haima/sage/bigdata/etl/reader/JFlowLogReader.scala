package com.haima.sage.bigdata.etl.reader

import java.io.IOException

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.NetSource
import com.haima.sage.bigdata.etl.stream.NetFlowStream

class JFlowLogReader(conf: NetSource) extends LogReader[RichMap] {

  val host: String = conf.host.getOrElse("0.0.0.0")
  val port: Int = if (conf.port == 0) 2025 else conf.port
  /*
    val cache: Int = if (conf.cache <= 0) 1000 else conf.cache

    if (conf.protocol == null) {
      throw new NullPointerException("protocol can`t null please set !")
    }*/

  override val stream = NetFlowStream(host, port,conf.timeout)

  def skip(skip: Long): Long = 0

  override def path: String = "://" + host + ":" + port

  @throws(classOf[IOException])
  override def close() {
    logger.debug(s"${conf.uri} closing")
    super.close()
    logger.debug(s"${conf.uri} closed")
  }
}