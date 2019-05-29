package com.haima.sage.bigdata.etl.utils

object HTTPAddress {

  lazy val reg = "((http[s]?)(://))?([^/]+):([\\d]+)/?(.+)?".r

  def apply(uri: String): Option[HTTPAddress] = {

    val data=reg.unapplySeq(uri)
    data match {
      case Some(_::protocol::_ :: host :: port :: url :: Nil) =>
        Some(HTTPAddress(Option(protocol), host, port.toInt, Option(url)))
      case Some(host :: port :: url :: Nil) if port.matches("\\d+") =>
        Some(HTTPAddress(None, host, port.toInt, Option(url)))
      case Some(_::protocol::_ :: host :: port :: Nil) if port.matches("\\d+") =>
        Some(HTTPAddress(Option(protocol), host, port.toInt))
      case Some(host :: port :: Nil) =>
        Some(HTTPAddress(None, host, port.toInt))
      case _ =>
        None
    }

  }

}

case class HTTPAddress(protocol: Option[String] = None, host: String, port: Int, url: Option[String] = None)
