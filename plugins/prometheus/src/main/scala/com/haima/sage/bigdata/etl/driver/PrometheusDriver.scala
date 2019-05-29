package com.haima.sage.bigdata.etl.driver

import java.util.Properties

import com.haima.sage.bigdata.etl.common.model.WithProperties
import com.haima.sage.bigdata.etl.utils.Logger
import java.net.Socket
import scala.util.Try
/**
  * Created by liyju on 2017/8/24.
  */
case class PrometheusDriver (mate :PrometheusMate) extends Driver[Socket] with WithProperties with Logger {
  override def driver(): Try[Socket] = Try{
    val socket = new Socket(mate.host, mate.port)
    socket.close()
    socket
  }
  override def properties: Option[Properties] =  mate.properties
}
